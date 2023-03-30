/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.get

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.node.NodeClient
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.commons.ConfigConstants
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.controlcenter.notification.LRONConfigResponse
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.util.getLRONConfigAndParse
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportGetLRONConfigAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<GetLRONConfigRequest, GetLRONConfigResponse>(
    GetLRONConfigAction.NAME, transportService, actionFilters, ::GetLRONConfigRequest
) {
    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: GetLRONConfigRequest, listener: ActionListener<GetLRONConfigResponse>) {
        GetLRONConfigHandler(client, listener, request).start()
    }

    inner class GetLRONConfigHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<GetLRONConfigResponse>,
        private val request: GetLRONConfigRequest
    ) {
        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            client.threadPool().threadContext.stashContext().use {
                if (null != request.docId) {
                    getLRONConfigAndParse(
                        client,
                        request.docId,
                        xContentRegistry,
                        object : ActionListener<LRONConfigResponse> {
                            override fun onResponse(response: LRONConfigResponse) {
                                actionListener.onResponse(GetLRONConfigResponse(listOf(response), 1))
                            }

                            override fun onFailure(e: java.lang.Exception) {
                                actionListener.onFailure(e)
                            }
                        }
                    )
                    return
                } else {
                    doSearch()
                }
            }
        }

        private fun doSearch() {
            val params = request.searchParams
            val sortBuilder = params!!.getSortBuilder()
            val queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.existsQuery("lron_config"))
                .must(QueryBuilders.queryStringQuery(params.queryString))

            val searchSourceBuilder = SearchSourceBuilder()
                .query(queryBuilder)
                .sort(sortBuilder)
                .from(params.from)
                .size(params.size)

            val searchRequest = SearchRequest()
                .source(searchSourceBuilder)
                .indices(IndexManagementPlugin.CONTROL_CENTER_INDEX)

            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        val totalNumber = response.hits.totalHits?.value ?: 0
                        val lronConfigResponses = response.hits.hits.map {
                            val xcp = XContentFactory.xContent(XContentType.JSON)
                                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, it.sourceAsString)
                            LRONConfigResponse(
                                id = it.id,
                                lronConfig = xcp.parseWithType(id = it.id, parse = LRONConfig.Companion::parse)
                            )
                        }
                        actionListener.onResponse(GetLRONConfigResponse(lronConfigResponses, totalNumber.toInt()))
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(t)
                    }
                }
            )
        }
    }
}
