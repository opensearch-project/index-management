/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.index

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.AdminPanelIndices
import org.opensearch.indexmanagement.adminpanel.notification.LRONConfigResponse
import org.opensearch.indexmanagement.adminpanel.notification.util.getDocID
import org.opensearch.indexmanagement.adminpanel.notification.util.getLRONConfigAndParse
import org.opensearch.indexmanagement.adminpanel.notification.util.getPriority
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

@Suppress("LongParameterList")
class TransportIndexLRONConfigAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val adminPanelIndices: AdminPanelIndices,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<IndexLRONConfigRequest, LRONConfigResponse>(
    IndexLRONConfigAction.NAME, transportService, actionFilters, ::IndexLRONConfigRequest
) {
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: IndexLRONConfigRequest, listener: ActionListener<LRONConfigResponse>) {
        IndexLRONConfigHandler(client, listener, request).start()
    }

    inner class IndexLRONConfigHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<LRONConfigResponse>,
        private val request: IndexLRONConfigRequest,
        private val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext),
        private val docId: String = getDocID(request.lronConfig.taskId, request.lronConfig.actionName)
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
                adminPanelIndices.checkAndUpdateIMConfigIndex(ActionListener.wrap(::onCreateMappingsResponse, actionListener::onFailure))
            }
            return
        }

        private fun onCreateMappingsResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated ${IndexManagementPlugin.ADMIN_PANEL_INDEX} with newest mappings.")
                validate()
            } else {
                val message = "Unable to create or update ${IndexManagementPlugin.ADMIN_PANEL_INDEX} with newest mapping."
                log.error(message)
                actionListener.onFailure(OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun validate() {
            if (request.isUpdate && filterByEnabled) {
                /* We need to verify whether the user has permissions for the resource (backend roles) */
                getLRONConfigAndParse(
                    client,
                    docId,
                    xContentRegistry,
                    object : ActionListener<LRONConfigResponse> {
                        override fun onResponse(response: LRONConfigResponse) {
                            if (!SecurityUtils.userHasPermissionForResource(
                                    user,
                                    response.lronConfig.user,
                                    filterByEnabled,
                                    "lronConfig",
                                    docId,
                                    actionListener
                                )
                            ) {
                                return
                            } else {
                                putLRONConfig()
                            }
                        }

                        override fun onFailure(e: java.lang.Exception) {
                            actionListener.onFailure(e)
                        }
                    }
                )
                return
            } else putLRONConfig()
        }

        private fun putLRONConfig() {
            val lronConfig = request.lronConfig.copy(
                user = this.user,
                priority = getPriority(request.lronConfig.taskId, request.lronConfig.actionName)
            )
            val indexRequest = IndexRequest(IndexManagementPlugin.ADMIN_PANEL_INDEX)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(lronConfig.toXContent(XContentFactory.jsonBuilder()))
                .id(docId)
                .timeout(IndexRequest.DEFAULT_TIMEOUT)
            if (!request.isUpdate) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            }

            client.index(
                indexRequest,
                object : ActionListener<IndexResponse> {
                    override fun onResponse(response: IndexResponse) {
                        if (response.shardInfo.failed > 0) {
                            val failureReasons = response.shardInfo.failures.joinToString(",") { it.reason() }
                            actionListener.onFailure(OpenSearchStatusException(failureReasons, response.status()))
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

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(e)
                    }
                }
            )
        }
    }
}
