/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.stop

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
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.util.parseRollup
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.lang.IllegalArgumentException
import java.time.Instant

/**
 * Disables a rollup job and updates the rollup metadata if required.
 *
 * Stopping a rollup job requires up to two calls to be done.
 * 1. Disable the job itself so it stops being scheduled and executed by job scheduler.
 * 2. Update the rollup metadata status to reflect that it is not running anymore.
 *
 * There are no transactions so we will attempt to do the calls serially with the second relying
 * on the first ones success. With that in mind it's better to update metadata first and rollup job second
 * as a metadata: successful and job: failed can be recovered from in the runner where it will disable the job.
 * The inverse (job: successful and metadata: fail) will end up with a disabled job and a metadata that potentially
 * says STARTED still which is wrong.
 */
@Suppress("LongParameterList")
class TransportStopRollupAction
@Inject
constructor(
    transportService: TransportService,
    val client: Client,
    val clusterService: ClusterService,
    val settings: Settings,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val pluginClient: PluginClient,
) : HandledTransportAction<StopRollupRequest, AcknowledgedResponse>(
    StopRollupAction.NAME, transportService, actionFilters, ::StopRollupRequest,
) {
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    @Suppress("ReturnCount")
    override fun doExecute(task: Task, request: StopRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        log.debug("Executing StopRollupAction on ${request.id}")
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
            )}",
        )
        val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.id)
        val user = buildUser(client.threadPool().threadContext)
        pluginClient.get(
            getRequest,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists) {
                        actionListener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                        return
                    }

                    val rollup: Rollup?
                    try {
                        rollup = parseRollup(response, xContentRegistry)
                    } catch (e: IllegalArgumentException) {
                        actionListener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                        return
                    }
                    if (!userHasPermissionForResource(user, rollup.user, filterByEnabled, "rollup", rollup.id, actionListener)) {
                        return
                    }
                    if (rollup.metadataID != null) {
                        getRollupMetadata(rollup, request, actionListener)
                    } else {
                        updateRollupJob(rollup, request, actionListener)
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            },
        )
    }

    private fun getRollupMetadata(rollup: Rollup, request: StopRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val req = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, rollup.metadataID).routing(rollup.id)
        pluginClient.get(
            req,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists || response.isSourceEmpty) {
                        // If there is no metadata there is nothing to stop, proceed to disable job
                        updateRollupJob(rollup, request, actionListener)
                    } else {
                        val metadata =
                            response.sourceAsBytesRef?.let {
                                val xcp =
                                    XContentHelper.createParser(
                                        NamedXContentRegistry.EMPTY,
                                        LoggingDeprecationHandler.INSTANCE, it, XContentType.JSON,
                                    )
                                xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, RollupMetadata.Companion::parse)
                            }
                        if (metadata == null) {
                            // If there is no metadata there is nothing to stop, proceed to disable job
                            updateRollupJob(rollup, request, actionListener)
                        } else {
                            updateRollupMetadata(rollup, metadata, request, actionListener)
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            },
        )
    }

    /**
     * Updates the rollup metadata if required.
     *
     * The update is dependent on what the current [RollupMetadata.status] is.
     * When stopping a rollup that is in INIT, STARTED, or STOPPED we will update to STOPPED.
     * When the rollup is in FINISHED or FAILED it will remain as that status.
     * When the rollup is in RETRY we will update it back to a FAILED status so that it can correctly go back into RETRY if needed.
     */
    private fun updateRollupMetadata(
        rollup: Rollup,
        metadata: RollupMetadata,
        request: StopRollupRequest,
        actionListener: ActionListener<AcknowledgedResponse>,
    ) {
        val now = Instant.now().toEpochMilli()
        val updatedStatus =
            when (metadata.status) {
                RollupMetadata.Status.STARTED, RollupMetadata.Status.INIT, RollupMetadata.Status.STOPPED -> RollupMetadata.Status.STOPPED
                RollupMetadata.Status.FINISHED, RollupMetadata.Status.FAILED -> metadata.status
                RollupMetadata.Status.RETRY -> RollupMetadata.Status.FAILED
            }
        val failureReason =
            if (metadata.status == RollupMetadata.Status.RETRY) {
                "Stopped a rollup that was in retry, rolling back to failed status"
            } else {
                null
            }
        val updateRequest =
            UpdateRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, rollup.metadataID)
                .doc(
                    mapOf(
                        RollupMetadata.ROLLUP_METADATA_TYPE to
                            mapOf(
                                RollupMetadata.STATUS_FIELD to updatedStatus.type,
                                RollupMetadata.FAILURE_REASON to failureReason, RollupMetadata.LAST_UPDATED_FIELD to now,
                            ),
                    ),
                )
                .routing(rollup.id)
        updateRequest.refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        pluginClient.update(
            updateRequest,
            object : ActionListener<UpdateResponse> {
                override fun onResponse(response: UpdateResponse) {
                    if (response.result == DocWriteResponse.Result.UPDATED) {
                        updateRollupJob(rollup, request, actionListener)
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

    private fun updateRollupJob(rollup: Rollup, request: StopRollupRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val now = Instant.now().toEpochMilli()
        val updateReq = UpdateRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.id)
        updateReq.setIfSeqNo(rollup.seqNo).setIfPrimaryTerm(rollup.primaryTerm)
            .doc(
                mapOf(
                    Rollup.ROLLUP_TYPE to
                        mapOf(
                            Rollup.ENABLED_FIELD to false,
                            Rollup.ENABLED_TIME_FIELD to null, Rollup.LAST_UPDATED_TIME_FIELD to now,
                        ),
                ),
            )
            .routing(rollup.id)
        updateReq.refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        pluginClient.update(
            updateReq,
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
