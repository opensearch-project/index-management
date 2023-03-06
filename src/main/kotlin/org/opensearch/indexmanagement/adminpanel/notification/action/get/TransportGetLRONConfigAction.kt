/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.get

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.LRONConfigResponse
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportGetLRONConfigAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<GetLRONConfigRequest, LRONConfigResponse>(
    GetLRONConfigAction.NAME, transportService, actionFilters, ::GetLRONConfigRequest
) {
    @Volatile
    private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: GetLRONConfigRequest, listener: ActionListener<LRONConfigResponse>) {
        GetLRONConfigHandler(client, listener, request).start()
    }

    inner class GetLRONConfigHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<LRONConfigResponse>,
        private val request: GetLRONConfigRequest,
        private val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext)
    ) {
        fun start() {
            val threadContext = client.threadPool().threadContext
            log.debug(
                "User and roles string from thread context: ${threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)}"
            )
            threadContext.stashContext().use {
                if (!SecurityUtils.validateUserConfiguration(user, filterByEnabled, actionListener)) {
                    return
                }
                val getRequest = GetRequest().id(request.docID).index(IndexManagementPlugin.ADMIN_PANEL_INDEX)
                client.get(getRequest, ActionListener.wrap(::onGetResponse, actionListener::onFailure))
            }
            return
        }

        private fun onGetResponse(response: GetResponse) {
            if (!response.isExists) {
                actionListener.onFailure(OpenSearchStatusException("lronConfig ${request.docID} not found", RestStatus.NOT_FOUND))
                return
            }

            val lronConfig: LRONConfig

            try {
                lronConfig = parseFromGetResponse(response, xContentRegistry, LRONConfig.Companion::parse)
            } catch (e: IllegalArgumentException) {
                actionListener.onFailure(e)
                return
            }

            if (!SecurityUtils.userHasPermissionForResource(
                    user,
                    lronConfig.user,
                    filterByEnabled,
                    "lronConfig",
                    request.docID,
                    actionListener
                )
            ) {
                return
            } else {
                actionListener.onResponse(
                    LRONConfigResponse(
                        response.id,
                        response.version,
                        response.primaryTerm,
                        response.seqNo,
                        lronConfig
                    )
                )
            }
        }
    }
}
