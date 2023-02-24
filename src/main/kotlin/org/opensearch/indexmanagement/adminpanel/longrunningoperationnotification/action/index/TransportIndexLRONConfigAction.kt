/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.action.index

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.AdminPanelIndices
import org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.util.LRON_DEFAULT_ID
import org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.util.LRON_DOC_ID_PREFIX
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportIndexLRONConfigAction::class.java)

@Suppress("LongParameterList")
class TransportIndexLRONConfigAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val adminPanelIndices: AdminPanelIndices,
    val clusterService: ClusterService,
    val settings: Settings
) : HandledTransportAction<IndexLRONConfigRequest, IndexLRONConfigResponse>(
    IndexLRONConfigAction.NAME, transportService, actionFilters, ::IndexLRONConfigRequest
) {
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: IndexLRONConfigRequest, listener: ActionListener<IndexLRONConfigResponse>) {
        IndexLRONConfigHandler(client, listener, request).start()
    }

    inner class IndexLRONConfigHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<IndexLRONConfigResponse>,
        private val request: IndexLRONConfigRequest,
        private val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext)
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
                if (request.taskID == NO_ID) {
                    putLRONConfig()
                } else {
                    validateTaskID()
                }
            } else {
                val message = "Unable to create or update ${IndexManagementPlugin.ADMIN_PANEL_INDEX} with newest mapping."
                log.error(message)
                actionListener.onFailure(OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun validateTaskID() {
            putLRONConfig()
        }

        private fun generateID(): String {
            if (NO_ID == request.taskID) {
                return LRON_DOC_ID_PREFIX + LRON_DEFAULT_ID
            } else {
                return LRON_DOC_ID_PREFIX + request.taskID
            }
        }

        private fun putLRONConfig() {
            val docID = generateID()
            val lronConfig = request.lronConfig.copy(user = this.user)
            val indexRequest = IndexRequest(IndexManagementPlugin.ADMIN_PANEL_INDEX)
                .setRefreshPolicy(request.refreshPolicy)
                .source(lronConfig.toXContent(XContentFactory.jsonBuilder()))
                .id(docID)
                .timeout(IndexRequest.DEFAULT_TIMEOUT)

            if (request.seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || request.primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            } else {
                indexRequest.setIfSeqNo(request.seqNo)
                    .setIfPrimaryTerm(request.primaryTerm)
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
                                IndexLRONConfigResponse(
                                    response.id,
                                    response.version,
                                    response.primaryTerm,
                                    response.seqNo,
                                    response.status()
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
