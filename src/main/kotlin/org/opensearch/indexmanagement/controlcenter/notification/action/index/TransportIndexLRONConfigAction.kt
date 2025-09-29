/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.index

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.controlcenter.notification.ControlCenterIndices
import org.opensearch.indexmanagement.controlcenter.notification.LRONConfigResponse
import org.opensearch.indexmanagement.controlcenter.notification.util.getDocID
import org.opensearch.indexmanagement.controlcenter.notification.util.getPriority
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

@Suppress("LongParameterList")
class TransportIndexLRONConfigAction
@Inject
constructor(
    val client: PluginClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val controlCenterIndices: ControlCenterIndices,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<IndexLRONConfigRequest, LRONConfigResponse>(
    IndexLRONConfigAction.NAME, transportService, actionFilters, ::IndexLRONConfigRequest,
) {
    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexLRONConfigRequest, listener: ActionListener<LRONConfigResponse>) {
        IndexLRONConfigHandler(client, listener, request).start()
    }

    inner class IndexLRONConfigHandler(
        private val client: Client,
        private val actionListener: ActionListener<LRONConfigResponse>,
        private val request: IndexLRONConfigRequest,
        private val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext),
        private val docId: String = getDocID(request.lronConfig.taskId, request.lronConfig.actionName),
    ) {
        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
                )}",
            )
            // we use dryRun to help check permission and do request validation
            if (request.dryRun) {
                validate()
                return
            }
            controlCenterIndices.checkAndUpdateControlCenterIndex(
                ActionListener.wrap(::onCreateMappingsResponse, actionListener::onFailure),
            )
        }

        private fun onCreateMappingsResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated ${IndexManagementPlugin.CONTROL_CENTER_INDEX} with newest mappings.")
                validate()
            } else {
                val message = "Unable to create or update ${IndexManagementPlugin.CONTROL_CENTER_INDEX} with newest mapping."
                log.error(message)
                actionListener.onFailure(OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun validate() {
            // check whether the node id in task id exists
            if (null != request.lronConfig.taskId && null == clusterService.state().nodes.get(request.lronConfig.taskId.nodeId)) {
                actionListener.onFailure(IllegalArgumentException("Illegal taskID. NodeID not exists."))
                return
            }
            putLRONConfig()
        }

        private fun putLRONConfig() {
            val lronConfig =
                request.lronConfig.copy(
                    user = this.user,
                    priority = getPriority(request.lronConfig.taskId, request.lronConfig.actionName),
                )

            if (request.dryRun) {
                actionListener.onResponse(LRONConfigResponse(docId, lronConfig))
                return
            }

            val indexRequest =
                IndexRequest(IndexManagementPlugin.CONTROL_CENTER_INDEX)
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
                                    lronConfig,
                                ),
                            )
                        }
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    }
                },
            )
        }
    }
}
