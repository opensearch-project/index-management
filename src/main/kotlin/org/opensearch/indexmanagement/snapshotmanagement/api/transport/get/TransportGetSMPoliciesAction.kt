/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.get

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.ExistsQueryBuilder
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import org.opensearch.indexmanagement.opensearchapi.contentParser
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICIES_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.TransportService

class TransportGetSMPoliciesAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
) : BaseTransportAction<GetSMPoliciesRequest, GetSMPoliciesResponse>(
    GET_SM_POLICIES_ACTION_NAME, transportService, client, actionFilters, ::GetSMPoliciesRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override suspend fun executeRequest(
        request: GetSMPoliciesRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): GetSMPoliciesResponse {
        val (policies, totalPoliciesCount) = getAllPolicies()

        return GetSMPoliciesResponse(policies, totalPoliciesCount)
    }

    private suspend fun getAllPolicies(): Pair<List<SMPolicy>, Long> {
        val searchRequest = getAllPoliciesRequest()
        val searchResponse: SearchResponse = try {
            client.suspendUntil { search(searchRequest, it) }
        } catch (e: IndexNotFoundException) {
            throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        }
        return parseGetAllPoliciesResponse(searchResponse)
    }

    private fun getAllPoliciesRequest(): SearchRequest {
        val queryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(SMPolicy.SM_TYPE))
        // TODO SM add user filter
        val searchSourceBuilder = SearchSourceBuilder().size(MAX_HITS).query(queryBuilder).seqNoAndPrimaryTerm(true)
        return SearchRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
    }

    private fun parseGetAllPoliciesResponse(searchResponse: SearchResponse): Pair<List<SMPolicy>, Long> {
        return try {
            val totalPolicies = searchResponse.hits.totalHits?.value ?: 0L
            searchResponse.hits.hits.map {
                contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, SMPolicy.Companion::parse)
            } to totalPolicies
        } catch (e: Exception) {
            log.error("Failed to parse snapshot management policy in search response", e)
            throw OpenSearchStatusException("Failed to parse snapshot management policy", RestStatus.NOT_FOUND)
        }
    }
}
