/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * com.maddyhome.idea.copyright.pattern.CommentInfo@6331d08d
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain

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
import org.opensearch.index.query.WildcardQueryBuilder
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import org.opensearch.indexmanagement.opensearchapi.contentParser
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.model.ExplainSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.SM_METADATA_TYPE
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.NAME_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.smMetadataDocIdToPolicyName
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.TransportService

class TransportExplainSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters
) : BaseTransportAction<ExplainSMPolicyRequest, ExplainSMPolicyResponse>(
    SMActions.EXPLAIN_SM_ACTION_NAME, transportService, client, actionFilters, ::ExplainSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override suspend fun executeRequest(
        request: ExplainSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): ExplainSMPolicyResponse {
        val policyNames = request.smPolicyNames

        val namesToEnabled = getPolicyEnabledStatus(policyNames.toSet())
        val namesToMetadata = getSMMetadata(namesToEnabled.keys)
        return buildExplainResponse(namesToEnabled, namesToMetadata)
    }

    private suspend fun getPolicyEnabledStatus(policyNames: Set<String>): Map<String, Boolean> {
        // Search for all SM Policy documents which match at least one of the given names
        val queryBuilder = BoolQueryBuilder().minimumShouldMatch(1).apply {
            policyNames.forEach {
                this.should(WildcardQueryBuilder("${SMPolicy.SM_TYPE}.$NAME_FIELD.keyword", it))
            }
        }
        // TODO SM add user filter

        // TODO CLAY only get the enabled field
        // Search the config index for SM policies
        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().size(MAX_HITS).query(queryBuilder))
        val searchResponse: SearchResponse = try {
            client.suspendUntil { search(searchRequest) }
        } catch (e: IndexNotFoundException) {
            throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        }

        // Parse each returned policy to get the job enabled status
        return try {
            searchResponse.hits.hits.associate {
                val smPolicy = contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, SMPolicy.Companion::parse)
                smPolicy.getSMPolicyName() to smPolicy.jobEnabled
            }
        } catch (e: Exception) {
            log.error("Failed to parse snapshot management policy in search response", e)
            throw OpenSearchStatusException("Failed to parse snapshot management policy", RestStatus.NOT_FOUND)
        }
    }

    private suspend fun getSMMetadata(policyNames: Set<String>): Map<String, SMMetadata> {
        // Search for all SM Metadata documents which match at least one of the given names
        val queryBuilder = BoolQueryBuilder().minimumShouldMatch(1).apply {
            policyNames.forEach {
                this.should(WildcardQueryBuilder("$SM_METADATA_TYPE.$NAME_FIELD.keyword", it))
            }
        }

        // Search the config index for SM Metadata
        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().size(MAX_HITS).query(queryBuilder))
        val searchResponse: SearchResponse = try {
            client.suspendUntil { search(searchRequest) }
        } catch (e: IndexNotFoundException) {
            throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        }

        return try {
            searchResponse.hits.hits.associate {
                val smMetadata = contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, SMMetadata.Companion::parse)
                smMetadataDocIdToPolicyName(smMetadata.id) to smMetadata
            }
        } catch (e: Exception) {
            log.error("Failed to parse snapshot management metadata in search response", e)
            throw OpenSearchStatusException("Failed to parse snapshot management metadata", RestStatus.NOT_FOUND)
        }
    }

    private fun buildExplainResponse(namesToEnabled: Map<String, Boolean>, namesToMetadata: Map<String, SMMetadata>): ExplainSMPolicyResponse {
        val policiesToExplain = namesToEnabled.entries.associate { (policyName, enabled) ->
            policyName to ExplainSMPolicy(policyName, namesToMetadata[policyName], enabled)
        }
        return ExplainSMPolicyResponse(policiesToExplain)
    }
}
