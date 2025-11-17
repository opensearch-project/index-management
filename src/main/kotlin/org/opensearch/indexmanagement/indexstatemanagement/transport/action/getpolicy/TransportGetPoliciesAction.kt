/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.cluster.routing.Preference
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.ConfigConstants
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.opensearchapi.parseFromSearchResponse
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.addUserFilter
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportGetPoliciesAction::class.java)

@Suppress("LongParameterList")
class TransportGetPoliciesAction
@Inject
constructor(
    transportService: TransportService,
    val client: PluginClient,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
) : HandledTransportAction<GetPoliciesRequest, GetPoliciesResponse>(
    GetPoliciesAction.NAME, transportService, actionFilters, ::GetPoliciesRequest,
) {
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(
        task: Task,
        getPoliciesRequest: GetPoliciesRequest,
        actionListener: ActionListener<GetPoliciesResponse>,
    ) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
            )}",
        )
        val params = getPoliciesRequest.searchParams
        val user = buildUser(client.threadPool().threadContext)

        val sortBuilder = params.getSortBuilder()

        val queryBuilder =
            QueryBuilders.boolQuery()
                .must(QueryBuilders.existsQuery("policy"))

        // Add user filter if enabled
        addUserFilter(user, queryBuilder, filterByEnabled, "policy.user")

        queryBuilder.must(
            QueryBuilders
                .queryStringQuery(params.queryString)
                .defaultOperator(Operator.AND)
                .field("policy.policy_id.keyword"),
        )

        val searchSourceBuilder =
            SearchSourceBuilder()
                .query(queryBuilder)
                .sort(sortBuilder)
                .from(params.from)
                .size(params.size)
                .seqNoAndPrimaryTerm(true)

        val searchRequest =
            SearchRequest()
                .source(searchSourceBuilder)
                .indices(INDEX_MANAGEMENT_INDEX)
                .preference(Preference.PRIMARY_FIRST.type())

        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    val totalPolicies = response.hits.totalHits?.value ?: 0
                    val policies = parseFromSearchResponse(response, xContentRegistry, Policy.Companion::parse)
                    actionListener.onResponse(GetPoliciesResponse(policies, totalPolicies.toInt()))
                }

                override fun onFailure(t: Exception) {
                    if (t is IndexNotFoundException) {
                        // config index hasn't been initialized, catch this here and show empty result on Kibana
                        actionListener.onResponse(GetPoliciesResponse(emptyList(), 0))
                        return
                    }
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                }
            },
        )
    }
}
