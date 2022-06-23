/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.delete

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.opensearch.rest.RestStatus
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

@Suppress("ReturnCount")
class TransportDeleteTransformsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    actionFilters: ActionFilters
) : HandledTransportAction<DeleteTransformsRequest, BulkResponse>(
    DeleteTransformsAction.NAME, transportService, actionFilters, ::DeleteTransformsRequest
) {

    private val log = LogManager.getLogger(javaClass)
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: DeleteTransformsRequest, actionListener: ActionListener<BulkResponse>) {
        // TODO: if metadata id exists delete the metadata doc else just delete transform
        DeleteTransformHandler(client, request, actionListener).start()
    }

    inner class DeleteTransformHandler(
        val client: Client,
        val request: DeleteTransformsRequest,
        val actionListener: ActionListener<BulkResponse>,
        val user: User? = buildUser(client.threadPool().threadContext)
    ) {

        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            // Use Multi-Get Request
            val getRequest = MultiGetRequest()
            val fetchSourceContext = FetchSourceContext(true)
            request.ids.forEach { id ->
                getRequest.add(MultiGetRequest.Item(INDEX_MANAGEMENT_INDEX, id).fetchSourceContext(fetchSourceContext))
            }

            client.threadPool().threadContext.stashContext().use {
                client.multiGet(
                    getRequest,
                    object : ActionListener<MultiGetResponse> {
                        override fun onResponse(response: MultiGetResponse) {
                            try {
                                // response is failed only if managed index is not present
                                if (response.responses.first().isFailed) {
                                    actionListener.onFailure(
                                        OpenSearchStatusException(
                                            "Cluster missing system index $INDEX_MANAGEMENT_INDEX, cannot execute the request", RestStatus.BAD_REQUEST
                                        )
                                    )
                                    return
                                }

                                bulkDelete(response, request.ids, request.force, actionListener)
                            } catch (e: Exception) {
                                actionListener.onFailure(e)
                            }
                        }

                        override fun onFailure(e: Exception) = actionListener.onFailure(e)
                    }
                )
            }
        }

        @Suppress("LongMethod")
        private fun bulkDelete(response: MultiGetResponse, ids: List<String>, forceDelete: Boolean, actionListener: ActionListener<BulkResponse>) {
            val enabledIDs = mutableListOf<String>()
            val notTransform = mutableListOf<String>()
            val noPermission = mutableListOf<String>()

            response.responses.forEach {
                if (it.response.isExists) {
                    try {
                        val transform = parseFromGetResponse(it.response, xContentRegistry, Transform.Companion::parse)
                        val enabled = transform.enabled
                        if (enabled && !forceDelete) {
                            enabledIDs.add(it.id)
                        }
                        if (!userHasPermissionForResource(user, transform.user, filterByEnabled)) {
                            noPermission.add(it.id)
                        }
                    } catch (e: Exception) {
                        // if cannot parse considering not a transform
                        notTransform.add(it.id)
                    }
                }
            }

            if (noPermission.isNotEmpty()) {
                actionListener.onFailure(
                    OpenSearchStatusException(
                        "Don't have permission to delete some/all transforms in [${request.ids}]", RestStatus.FORBIDDEN
                    )
                )
                return
            }

            if (notTransform.isNotEmpty()) {
                actionListener.onFailure(
                    OpenSearchStatusException(
                        "Cannot find transforms $notTransform", RestStatus.BAD_REQUEST
                    )
                )
                return
            }

            if (enabledIDs.isNotEmpty()) {
                actionListener.onFailure(
                    OpenSearchStatusException(
                        "$enabledIDs transform(s) are enabled, please disable them before deleting them or set force flag", RestStatus.CONFLICT
                    )
                )
                return
            }

            val bulkDeleteRequest = BulkRequest()
            bulkDeleteRequest.refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
            for (id in ids) {
                bulkDeleteRequest.add(DeleteRequest(INDEX_MANAGEMENT_INDEX, id))
            }

            client.bulk(
                bulkDeleteRequest,
                object : ActionListener<BulkResponse> {
                    override fun onResponse(response: BulkResponse) {
                        actionListener.onResponse(response)
                    }

                    override fun onFailure(e: Exception) = actionListener.onFailure(e)
                }
            )
        }
    }
}
