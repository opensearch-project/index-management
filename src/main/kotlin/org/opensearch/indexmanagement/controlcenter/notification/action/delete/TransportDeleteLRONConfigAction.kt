/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.delete

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.common.inject.Inject
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.controlcenter.notification.LRONConfigResponse
import org.opensearch.indexmanagement.controlcenter.notification.util.getLRONConfigAndParse
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.Exception

class TransportDeleteLRONConfigAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<DeleteLRONConfigRequest, DeleteResponse>(
    DeleteLRONConfigAction.NAME, transportService, actionFilters, ::DeleteLRONConfigRequest
) {
    private val log = LogManager.getLogger(javaClass)

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

            client.threadPool().threadContext.stashContext().use {
                getLRONConfigAndParse(
                    client,
                    request.docId,
                    xContentRegistry,
                    object : ActionListener<LRONConfigResponse> {
                        override fun onResponse(response: LRONConfigResponse) {
                            executeDelete()
                        }

                        override fun onFailure(e: Exception) {
                            actionListener.onFailure(e)
                        }
                    }
                )
            }
        }

        fun executeDelete() {
            val deleteRequest = DeleteRequest(IndexManagementPlugin.CONTROL_CENTER_INDEX, docId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)

            client.delete(deleteRequest, actionListener)
        }
    }
}
