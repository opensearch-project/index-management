/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.delete

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.LRONConfigResponse
import org.opensearch.indexmanagement.adminpanel.notification.util.getLRONConfigAndParse
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.Exception

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
        private val docId: String = request.docId
    ) {
        fun start() {
            log.debug(
                "User and roles string from thread context: ${
                client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )
                }"
            )

            if (!SecurityUtils.validateUserConfiguration(user, filterByEnabled, actionListener)) {
                return
            }

            client.threadPool().threadContext.stashContext().use {
                getLRONConfigAndParse(
                    client,
                    request.docId,
                    xContentRegistry,
                    object : ActionListener<LRONConfigResponse> {
                        override fun onResponse(response: LRONConfigResponse) {
                            if (!SecurityUtils.userHasPermissionForResource(
                                    user,
                                    response.lronConfig.user,
                                    filterByEnabled,
                                    "lronConfig",
                                    request.docId,
                                    actionListener
                                )
                            ) {
                                return
                            } else {
                                executeDelete()
                            }
                        }

                        override fun onFailure(e: Exception) {
                            actionListener.onFailure(e)
                        }
                    }
                )
            }
        }

        fun executeDelete() {
            val deleteRequest = DeleteRequest(IndexManagementPlugin.ADMIN_PANEL_INDEX, docId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)

            client.delete(deleteRequest, actionListener)
        }
    }
}
