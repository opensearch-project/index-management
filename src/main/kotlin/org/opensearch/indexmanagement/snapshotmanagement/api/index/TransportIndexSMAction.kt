/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.index

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.node.NodeClient
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.snapshotmanagement.getSMDocId
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.Exception

class TransportIndexSMAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
) : HandledTransportAction<IndexSMRequest, IndexSMResponse>(
    IndexSMAction.NAME, transportService, actionFilters, ::IndexSMRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexSMRequest, listener: ActionListener<IndexSMResponse>) {
        IndexPolicyHandler(client, listener, request).start()
    }

    inner class IndexPolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<IndexSMResponse>,
        private val request: IndexSMRequest,
    ) {
        val policy = request.smPolicy

        fun start() {
            log.info("slm build request")
            val indexReq = IndexRequest(INDEX_MANAGEMENT_INDEX)
                .source(policy.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .id(getSMDocId(policy.policyName))

            client.index(
                indexReq,
                object : ActionListener<IndexResponse> {
                    override fun onResponse(response: IndexResponse) {
                        log.info("slm index response: $response")
                        actionListener.onResponse(
                            IndexSMResponse(policy)
                        )
                    }

                    override fun onFailure(e: Exception) {
                    }
                }
            )
        }
    }
}
