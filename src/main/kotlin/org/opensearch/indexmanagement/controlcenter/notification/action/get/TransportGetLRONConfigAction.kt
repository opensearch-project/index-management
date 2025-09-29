/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.get

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.cluster.routing.Preference
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.controlcenter.notification.LRONConfigResponse
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.util.getLRONConfigAndParse
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

class TransportGetLRONConfigAction
@Inject
constructor(
    val client: PluginClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<GetLRONConfigRequest, GetLRONConfigResponse>(
    GetLRONConfigAction.NAME, transportService, actionFilters, ::GetLRONConfigRequest,
) {
    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: GetLRONConfigRequest, listener: ActionListener<GetLRONConfigResponse>) {
        GetLRONConfigHandler(client, listener, request).start()
    }

    inner class GetLRONConfigHandler(
        private val client: Client,
        private val actionListener: ActionListener<GetLRONConfigResponse>,
        private val request: GetLRONConfigRequest,
    ) {
        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
                )}",
            )
            if (null != request.docId) {
                getLRONConfigAndParse(
                    client,
                    request.docId,
                    xContentRegistry,
                    object : ActionListener<LRONConfigResponse> {
                        override fun onResponse(response: LRONConfigResponse) {
                            actionListener.onResponse(GetLRONConfigResponse(listOf(response), 1))
                        }

                        override fun onFailure(e: Exception) {
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                        }
                    },
                )
            } else {
                doSearch()
            }
        }

        private fun doSearch() {
            val params = request.searchParams
            val sortBuilder = params!!.getSortBuilder()
            val queryBuilder =
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.existsQuery("lron_config"))
                    .must(QueryBuilders.queryStringQuery(params.queryString))

            val searchSourceBuilder =
                SearchSourceBuilder()
                    .query(queryBuilder)
                    .sort(sortBuilder)
                    .from(params.from)
                    .size(params.size)

            val searchRequest =
                SearchRequest()
                    .source(searchSourceBuilder)
                    .indices(IndexManagementPlugin.CONTROL_CENTER_INDEX)
                    .preference(Preference.PRIMARY_FIRST.type())

            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        val totalNumber = response.hits.totalHits?.value ?: 0
                        val lronConfigResponses =
                            response.hits.hits.map {
                                val xcp =
                                    XContentHelper.createParser(
                                        xContentRegistry,
                                        LoggingDeprecationHandler.INSTANCE, it.sourceRef, XContentType.JSON,
                                    )
                                LRONConfigResponse(
                                    id = it.id,
                                    lronConfig = xcp.parseWithType(id = it.id, parse = LRONConfig.Companion::parse),
                                )
                            }
                        actionListener.onResponse(GetLRONConfigResponse(lronConfigResponses, totalNumber.toInt()))
                    }

                    override fun onFailure(e: Exception) {
                        if (e is IndexNotFoundException) {
                            // config index hasn't been initialized, catch this here and show empty result
                            actionListener.onResponse(GetLRONConfigResponse(emptyList(), 0))
                            return
                        }
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    }
                },
            )
        }
    }
}
