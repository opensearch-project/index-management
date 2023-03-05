package org.opensearch.indexmanagement.adminpanel.notification.action.delete

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.util.getDocID
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.IllegalArgumentException

class TransportDeleteLRONConfigAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<DeleteLRONConfigRequest, DeleteResponse>(
    DeleteLRONConfigAction.NAME, transportService, actionFilters, ::DeleteLRONConfigRequest
) {
    @Volatile
    private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: DeleteLRONConfigRequest, listener: ActionListener<DeleteResponse>) {
        DeleteLRONConfigHandler(client, listener, request).start()
    }

    inner class DeleteLRONConfigHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<DeleteResponse>,
        private val request: DeleteLRONConfigRequest,
        private val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext),
        private val docID: String = getDocID(request.taskID)
    ) {
        fun start() {
            log.debug(
                "User and roles string from thread context: ${
                client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )
                }"
            )
            client.threadPool().threadContext.stashContext().use {
                if (!SecurityUtils.validateUserConfiguration(user, filterByEnabled, actionListener)) {
                    return
                }
                if (!filterByEnabled) {
                    executeDelete()
                } else {
                    val getRequest = GetRequest(IndexManagementPlugin.ADMIN_PANEL_INDEX, docID)
                    client.get(
                        getRequest,
                        object : ActionListener<GetResponse> {
                            override fun onResponse(response: GetResponse) {
                                if (!response.isExists) {
                                    actionListener.onFailure(
                                        OpenSearchStatusException(
                                            "LRONConfig $docID not found",
                                            RestStatus.NOT_FOUND
                                        )
                                    )
                                    return
                                }

                                val lronConfig: LRONConfig?
                                try {
                                    lronConfig =
                                        parseFromGetResponse(response, xContentRegistry, LRONConfig.Companion::parse)
                                } catch (e: IllegalArgumentException) {
                                    actionListener.onFailure(
                                        OpenSearchStatusException(
                                            "Failed to parse LRONConfig $docID",
                                            RestStatus.NOT_FOUND
                                        )
                                    )
                                    return
                                }
                                if (!SecurityUtils.userHasPermissionForResource(
                                        user,
                                        lronConfig.user,
                                        filterByEnabled,
                                        "LRONConfig",
                                        docID,
                                        actionListener
                                    )
                                ) {
                                    return
                                } else {
                                    executeDelete()
                                }
                            }

                            override fun onFailure(t: Exception) {
                                actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                            }
                        }
                    )
                }
            }
            return
        }

        fun executeDelete() {
            val deleteRequest = DeleteRequest(IndexManagementPlugin.ADMIN_PANEL_INDEX, docID)
                .setRefreshPolicy(request.refreshPolicy)

            client.threadPool().threadContext.stashContext().use {
                client.delete(deleteRequest, actionListener)
            }
        }
    }
}
