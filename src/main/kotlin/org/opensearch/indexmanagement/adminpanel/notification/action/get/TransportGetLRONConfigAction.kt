/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.get

import GetLRONConfigsRequest
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.ActionListener
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.IdsQueryBuilder
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.opensearchapi.parseFromSearchResponse
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.indexmanagement.util._ID
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportGetLRONConfigAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<GetLRONConfigsRequest, GetLRONConfigsResponse>(
    GetLRONConfigAction.NAME, transportService, actionFilters, ::GetLRONConfigsRequest
) {
    @Volatile
    private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(
        task: Task,
        request: GetLRONConfigsRequest,
        listener: ActionListener<GetLRONConfigsResponse>
    ) {
        GetLRONConfigHandler(client, listener, request).start()
    }

    inner class GetLRONConfigHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<GetLRONConfigsResponse>,
        private val request: GetLRONConfigsRequest,
        private val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext),
        private val configTypes: MutableList<String> = mutableListOf()
    ) {
        fun start() {
            val threadContext = client.threadPool().threadContext
            log.debug(
                "User and roles string from thread context: ${threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)}"
            )
            threadContext.stashContext().use {
                if (!SecurityUtils.validateUserConfiguration(user, filterByEnabled, actionListener)) {
                    return
                }
                executeSearch()
            }
            return
        }

        private fun executeSearch() {
            val params = request.searchParams
            val user = SecurityUtils.buildUser(client.threadPool().threadContext)

            val sortBuilder = params.getSortBuilder()

            val queryBuilder = QueryBuilders.boolQuery()
                .must(
                    QueryBuilders
                        .queryStringQuery(params.queryString)
                        .defaultOperator(Operator.OR)
                        .field(_ID)
                )

            if (request.docIds.isNotEmpty()) {
                val idsQueryBuilder = IdsQueryBuilder().addIds(*request.docIds)
                queryBuilder.must(idsQueryBuilder)
            }

            // Add user filter if enabled
            SecurityUtils.addUserFilter(user, queryBuilder, filterByEnabled, "lron_config.user")

            val searchSourceBuilder = SearchSourceBuilder()
                .query(queryBuilder)
                .sort(sortBuilder)
                .from(params.from)
                .size(params.size)
                .seqNoAndPrimaryTerm(true)

            val searchRequest = SearchRequest()
                .source(searchSourceBuilder)
                .indices(IndexManagementPlugin.ADMIN_PANEL_INDEX)

            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        val totalPolicies = response.hits.totalHits?.value ?: 0
                        val lronConfigs =
                            parseFromSearchResponse(response, xContentRegistry, LRONConfig.Companion::parse)
                        actionListener.onResponse(GetLRONConfigsResponse(lronConfigs, totalPolicies.toInt(), false))
                    }

                    override fun onFailure(t: Exception) {
                        if (t is IndexNotFoundException) {
                            // config index hasn't been initialized, catch this here and show empty result on Kibana
                            actionListener.onResponse(GetLRONConfigsResponse(emptyList(), 0))
                            return
                        }
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }
    }
}
