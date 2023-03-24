/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.get

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.node.NodeClient
import org.opensearch.common.inject.Inject
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.commons.ConfigConstants
import org.opensearch.indexmanagement.controlcenter.notification.LRONConfigResponse
import org.opensearch.indexmanagement.controlcenter.notification.util.getLRONConfigAndParse
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportGetLRONConfigAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<GetLRONConfigRequest, LRONConfigResponse>(
    GetLRONConfigAction.NAME, transportService, actionFilters, ::GetLRONConfigRequest
) {
    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: GetLRONConfigRequest, listener: ActionListener<LRONConfigResponse>) {
        GetLRONConfigHandler(client, listener, request).start()
    }

    inner class GetLRONConfigHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<LRONConfigResponse>,
        private val request: GetLRONConfigRequest
    ) {
        fun start() {
            val threadContext = client.threadPool().threadContext
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            threadContext.stashContext().use {
                getLRONConfigAndParse(
                    client,
                    request.docId,
                    xContentRegistry,
                    object : ActionListener<LRONConfigResponse> {
                        override fun onResponse(response: LRONConfigResponse) {
                            actionListener.onResponse(response)
                        }

                        override fun onFailure(e: java.lang.Exception) {
                            actionListener.onFailure(e)
                        }
                    }
                )
            }
            return
        }
    }
}
