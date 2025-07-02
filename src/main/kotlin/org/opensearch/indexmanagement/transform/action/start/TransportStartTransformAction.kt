/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.start

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.update.UpdateRequest
import org.opensearch.action.update.UpdateResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.time.Instant

@Suppress("ReturnCount", "LongParameterList")
class TransportStartTransformAction
@Inject
constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val pluginClient: PluginClient,
) : HandledTransportAction<StartTransformRequest, AcknowledgedResponse>(
    StartTransformAction.NAME, transportService, actionFilters, ::StartTransformRequest,
) {
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: StartTransformRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
            )}",
        )
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id)
        val user = buildUser(client.threadPool().threadContext)
        pluginClient.get(
            getRequest,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists) {
                        actionListener.onFailure(OpenSearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                        return
                    }

                    val transform: Transform?
                    try {
                        transform = parseFromGetResponse(response, xContentRegistry, Transform.Companion::parse)
                    } catch (e: IllegalArgumentException) {
                        actionListener.onFailure(OpenSearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                        return
                    }

                    if (!userHasPermissionForResource(user, transform.user, filterByEnabled, "transform", transform.id, actionListener)) {
                        return
                    }
                    if (transform.enabled) {
                        log.debug("Transform job is already enabled, checking if metadata needs to be updated")
                        return if (transform.metadataId == null) {
                            actionListener.onResponse(AcknowledgedResponse(true))
                        } else {
                            retrieveAndUpdateTransformMetadata(transform, actionListener)
                        }
                    }

                    updateTransformJob(transform, request, actionListener)
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            },
        )
    }

    private fun updateTransformJob(
        transform: Transform,
        request: StartTransformRequest,
        actionListener: ActionListener<AcknowledgedResponse>,
    ) {
        val now = Instant.now().toEpochMilli()
        val updateReq = UpdateRequest(INDEX_MANAGEMENT_INDEX, request.id)
        updateReq.doc(
            mapOf(
                Transform.TRANSFORM_TYPE to
                    mapOf(
                        Transform.ENABLED_FIELD to true,
                        Transform.ENABLED_AT_FIELD to now, Transform.UPDATED_AT_FIELD to now,
                    ),
            ),
        )
        updateReq.refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        pluginClient.update(
            updateReq,
            object : ActionListener<UpdateResponse> {
                override fun onResponse(response: UpdateResponse) {
                    if (response.result == DocWriteResponse.Result.UPDATED) {
                        // If there is a metadata ID on transform then we need to set it back to STARTED or RETRY
                        if (transform.metadataId != null) {
                            retrieveAndUpdateTransformMetadata(transform, actionListener)
                        } else {
                            actionListener.onResponse(AcknowledgedResponse(true))
                        }
                    } else {
                        actionListener.onResponse(AcknowledgedResponse(false))
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            },
        )
    }

    private fun retrieveAndUpdateTransformMetadata(transform: Transform, actionListener: ActionListener<AcknowledgedResponse>) {
        val req = GetRequest(INDEX_MANAGEMENT_INDEX, transform.metadataId).routing(transform.id)
        pluginClient.get(
            req,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists || response.isSourceEmpty) {
                        actionListener.onFailure(OpenSearchStatusException("Metadata doc missing for transform [${req.id()}]", RestStatus.NOT_FOUND))
                    } else {
                        val metadata =
                            response.sourceAsBytesRef?.let {
                                val xcp =
                                    XContentHelper.createParser(
                                        NamedXContentRegistry.EMPTY,
                                        LoggingDeprecationHandler.INSTANCE, it, XContentType.JSON,
                                    )
                                xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, TransformMetadata.Companion::parse)
                            }
                        if (metadata == null) {
                            actionListener.onFailure(
                                OpenSearchStatusException(
                                    "Metadata doc missing for transform [${req.id()}]", RestStatus.NOT_FOUND,
                                ),
                            )
                        } else {
                            updateTransformMetadata(transform, metadata, actionListener)
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            },
        )
    }

    private fun updateTransformMetadata(transform: Transform, metadata: TransformMetadata, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        val updatedStatus =
            when (metadata.status) {
                TransformMetadata.Status.FINISHED, TransformMetadata.Status.STOPPED -> TransformMetadata.Status.STARTED
                TransformMetadata.Status.STARTED, TransformMetadata.Status.INIT ->
                    return actionListener.onResponse(AcknowledgedResponse(true))
                TransformMetadata.Status.FAILED -> TransformMetadata.Status.STARTED
            }
        val updateRequest =
            UpdateRequest(INDEX_MANAGEMENT_INDEX, transform.metadataId)
                .doc(
                    mapOf(
                        TransformMetadata.TRANSFORM_METADATA_TYPE to
                            mapOf(
                                TransformMetadata.STATUS_FIELD to updatedStatus.type,
                                TransformMetadata.FAILURE_REASON to null, TransformMetadata.LAST_UPDATED_AT_FIELD to now,
                            ),
                    ),
                )
                .routing(transform.id)
        updateRequest.refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        pluginClient.update(
            updateRequest,
            object : ActionListener<UpdateResponse> {
                override fun onResponse(response: UpdateResponse) {
                    actionListener.onResponse(AcknowledgedResponse(response.result == DocWriteResponse.Result.UPDATED))
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            },
        )
    }
}
