/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.get

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.get.MultiGetItemResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
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
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.util.LRON_DEFAULT_ID
import org.opensearch.indexmanagement.adminpanel.notification.util.LRON_DOC_ID_PREFIX
import org.opensearch.indexmanagement.adminpanel.notification.util.TYPE_DEFAULT
import org.opensearch.indexmanagement.adminpanel.notification.util.TYPE_TASK_ID
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.IllegalArgumentException

class TransportGetLRONConfigAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<GetLRONConfigRequest, GetLRONConfigResponse>(
    GetLRONConfigAction.NAME, transportService, actionFilters, ::GetLRONConfigRequest
) {
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: GetLRONConfigRequest, listener: ActionListener<GetLRONConfigResponse>) {
        GetLRONConfigHandler(client, listener, request).start()
    }

    inner class GetLRONConfigHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<GetLRONConfigResponse>,
        private val request: GetLRONConfigRequest,
        private val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext),
        private val configTypes: MutableList<String> = mutableListOf()
    ) {
        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            client.threadPool().threadContext.stashContext().use {
                if (!SecurityUtils.validateUserConfiguration(user, filterByEnabled, actionListener)) {
                    return
                }
                executeMultiGet()
            }
            return
        }

        fun executeMultiGet() {
            val multiGetRequest = MultiGetRequest()
            /* response includes a docs array that contains the documents in the order specified in the request */
            /* we add items in order of priority in request, and we just take the first non-null doc in response*/
            if (NO_ID != request.taskID) {
                multiGetRequest.add(IndexManagementPlugin.ADMIN_PANEL_INDEX, LRON_DOC_ID_PREFIX + request.taskID)
                configTypes.add(TYPE_TASK_ID)
            }
            if (request.includeDefault) {
                multiGetRequest.add(IndexManagementPlugin.ADMIN_PANEL_INDEX, LRON_DOC_ID_PREFIX + LRON_DEFAULT_ID)
                configTypes.add(TYPE_DEFAULT)
            }
            if (0 == multiGetRequest.items.size) {
                actionListener.onFailure(IllegalArgumentException("No task id and not include default configs"))
            }
            client.multiGet(multiGetRequest, ActionListener.wrap(::onMultiGetResponse, actionListener::onFailure))
        }

        fun onMultiGetResponse(multiGetResponse: MultiGetResponse) {
            /* All docs to get are in same index. If there is a failure, just return */
            for (response in multiGetResponse.responses) {
                if (null != response.failure) {
                    actionListener.onFailure(response.failure.failure)
                }
            }

            var lronConfig: LRONConfig?
            var response: MultiGetItemResponse?

            for (idx in multiGetResponse.responses.indices) {
                response = multiGetResponse.responses[idx]
                if (response.response.isExists) {
                    try {
                        lronConfig = parseFromGetResponse(response.response, xContentRegistry, LRONConfig.Companion::parse)
                    } catch (e: IllegalArgumentException) {
                        log.debug("can not parse LRONConfig: " + response.response.id)
                        continue
                    }
                    if (
                        !SecurityUtils.userHasPermissionForResource(
                            user,
                            lronConfig.user,
                            filterByEnabled,
                            "lron_config",
                            response.response.id, actionListener
                        )
                    ) {
                        return
                    }
                    actionListener.onResponse(
                        GetLRONConfigResponse(
                            id = response.response.id,
                            version = response.response.version,
                            primaryTerm = response.response.primaryTerm,
                            seqNo = response.response.seqNo,
                            configType = configTypes[idx],
                            lronConfig = lronConfig
                        )
                    )
                    return
                }
            }

            actionListener.onFailure(OpenSearchStatusException("LRONConfig not found", RestStatus.NOT_FOUND))
        }
    }
}
