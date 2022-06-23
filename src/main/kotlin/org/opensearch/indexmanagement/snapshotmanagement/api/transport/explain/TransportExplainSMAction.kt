/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.commons.authuser.User
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.ExistsQueryBuilder
import org.opensearch.index.query.TermQueryBuilder
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
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.ENABLED_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.NAME_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.opensearch.indexmanagement.snapshotmanagement.smMetadataDocIdToPolicyName
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.transport.TransportService

class TransportExplainSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<ExplainSMPolicyRequest, ExplainSMPolicyResponse>(
    SMActions.EXPLAIN_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::ExplainSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: ExplainSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): ExplainSMPolicyResponse {
        val policyNames = request.policyNames.toSet()

        val namesToEnabled = getPolicyEnabledStatus(policyNames, user)
        val namesToMetadata = getSMMetadata(namesToEnabled.keys)
        return buildExplainResponse(namesToEnabled, namesToMetadata)
    }

    @Suppress("ThrowsCount")
    private suspend fun getPolicyEnabledStatus(policyNames: Set<String>, user: User?): Map<String, Boolean> {
        // Search the config index for SM policies
        val searchRequest = getPolicyEnabledSearchRequest(policyNames, user)
        val searchResponse: SearchResponse = try {
            client.suspendUntil { search(searchRequest, it) }
        } catch (e: IndexNotFoundException) {
            throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        } catch (e: Exception) {
            log.error("Failed to search for snapshot management policy", e)
            throw OpenSearchStatusException("Failed to search for snapshot management policy", RestStatus.INTERNAL_SERVER_ERROR)
        }

        // Parse each returned policy to get the job enabled status
        return try {
            searchResponse.hits.hits.associate {
                parseNameToEnabled(contentParser(it.sourceRef))
            }
        } catch (e: Exception) {
            log.error("Failed to parse snapshot management policy in search response", e)
            throw OpenSearchStatusException("Failed to parse snapshot management policy", RestStatus.NOT_FOUND)
        }
    }

    private fun getPolicyEnabledSearchRequest(policyNames: Set<String>, user: User?): SearchRequest {
        val queryBuilder = getPolicyQuery(policyNames)

        // Add user filter if enabled
        SecurityUtils.addUserFilter(user, queryBuilder, filterByEnabled, "sm_policy.user")

        // Only return the name and enabled field
        val includes = arrayOf(
            "${SMPolicy.SM_TYPE}.$NAME_FIELD",
            "${SMPolicy.SM_TYPE}.$ENABLED_FIELD"
        )
        val fetchSourceContext = FetchSourceContext(true, includes, arrayOf())
        val searchSourceBuilder = SearchSourceBuilder().size(MAX_HITS).query(queryBuilder).fetchSource(fetchSourceContext)
        return SearchRequest(INDEX_MANAGEMENT_INDEX).source(searchSourceBuilder)
    }

    private fun getPolicyQuery(policyNames: Set<String>): BoolQueryBuilder {
        // Search for all SM Policy documents which match at least one of the given names
        val queryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(SMPolicy.SM_TYPE))
        queryBuilder.minimumShouldMatch(1).apply {
            policyNames.forEach { policyName ->
                if (policyName.contains('*') || policyName.contains('?')) {
                    this.should(WildcardQueryBuilder("${SMPolicy.SM_TYPE}.$NAME_FIELD", policyName))
                } else {
                    this.should(TermQueryBuilder("${SMPolicy.SM_TYPE}.$NAME_FIELD", policyName))
                }
            }
        }
        return queryBuilder
    }

    private suspend fun getSMMetadata(policyNames: Set<String>): Map<String, SMMetadata> {
        val searchRequest = getSMMetadataSearchRequest(policyNames)
        val searchResponse: SearchResponse = try {
            client.suspendUntil { search(searchRequest, it) }
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

    private fun getSMMetadataSearchRequest(policyNames: Set<String>): SearchRequest {
        // Search for all SM Metadata documents which match at least one of the given names
        val queryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(SM_METADATA_TYPE))
        queryBuilder.minimumShouldMatch(1).apply {
            policyNames.forEach {
                this.should(TermQueryBuilder("$SM_METADATA_TYPE.$NAME_FIELD", it))
            }
        }

        // Search the config index for SM Metadata
        return SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().size(MAX_HITS).query(queryBuilder))
    }

    private fun parseNameToEnabled(xcp: XContentParser): Pair<String, Boolean> {
        var name: String? = null
        var enabled: Boolean? = null

        if (xcp.currentToken() == null) xcp.nextToken()
        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                NAME_FIELD -> name = xcp.text()
                ENABLED_FIELD -> enabled = xcp.booleanValue()
            }
        }
        return requireNotNull(name) { "The name field of SMPolicy must not be null." } to
            requireNotNull(enabled) { "The enabled field of SMPolicy must not be null." }
    }

    private fun buildExplainResponse(namesToEnabled: Map<String, Boolean>, namesToMetadata: Map<String, SMMetadata>): ExplainSMPolicyResponse {
        val policiesToExplain = namesToEnabled.entries.associate { (policyName, enabled) ->
            policyName to ExplainSMPolicy(namesToMetadata[policyName], enabled)
        }
        log.debug("Explain response: $policiesToExplain")
        return ExplainSMPolicyResponse(policiesToExplain)
    }
}
