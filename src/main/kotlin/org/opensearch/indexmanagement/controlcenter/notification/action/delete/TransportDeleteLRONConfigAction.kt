/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.delete

import org.apache.logging.log4j.LogManager
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.inject.Inject
import org.opensearch.commons.ConfigConstants
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

class TransportDeleteLRONConfigAction
@Inject
constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    val client: PluginClient,
) : HandledTransportAction<DeleteLRONConfigRequest, DeleteResponse>(
    DeleteLRONConfigAction.NAME, transportService, actionFilters, ::DeleteLRONConfigRequest,
) {
    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: DeleteLRONConfigRequest, listener: ActionListener<DeleteResponse>) {
        DeleteLRONConfigHandler(client, listener, request).start()
    }

    inner class DeleteLRONConfigHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteResponse>,
        private val request: DeleteLRONConfigRequest,
        private val docId: String = request.docId,
    ) {
        fun start() {
            log.debug(
                "User and roles string from thread context: ${
                    client.threadPool().threadContext.getTransient<String>(
                        ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
                    )
                }",
            )

            val deleteRequest =
                DeleteRequest(IndexManagementPlugin.CONTROL_CENTER_INDEX, docId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)

            client.delete(deleteRequest, actionListener)
        }
    }
}
