/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.explain

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.ActionListener
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.ConfigConstants
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.IdsQueryBuilder
import org.opensearch.index.query.WildcardQueryBuilder
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import org.opensearch.indexmanagement.opensearchapi.contentParser
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.rollup.model.ExplainRollup
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.addUserFilter
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.TransportService
import kotlin.Exception

class TransportExplainRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    actionFilters: ActionFilters
) : HandledTransportAction<ExplainRollupRequest, ExplainRollupResponse>(
    ExplainRollupAction.NAME, transportService, actionFilters, ::ExplainRollupRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator")
    override fun doExecute(task: Task, request: ExplainRollupRequest, actionListener: ActionListener<ExplainRollupResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val ids = request.rollupIDs
        // Instantiate concrete ids to metadata map by removing wildcard matches
        val idsToExplain: MutableMap<String, ExplainRollup?> = ids.filter { !it.contains("*") }.map { it to null }.toMap(mutableMapOf())
        // First search is for all rollup documents that match at least one of the given rollupIDs
        val queryBuilder = BoolQueryBuilder().minimumShouldMatch(1).apply {
            ids.forEach {
                this.should(WildcardQueryBuilder("${Rollup.ROLLUP_TYPE}.${Rollup.ROLLUP_ID_FIELD}.keyword", "*$it*"))
            }
        }
        val user = buildUser(client.threadPool().threadContext)
        addUserFilter(user, queryBuilder, filterByEnabled, "rollup.user")

        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().size(MAX_HITS).query(queryBuilder))
        client.threadPool().threadContext.stashContext().use {
            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        try {
                            response.hits.hits.forEach {
                                val rollup = contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, Rollup.Companion::parse)
                                idsToExplain[rollup.id] = ExplainRollup(metadataID = rollup.metadataID)
                            }
                        } catch (e: Exception) {
                            log.error("Failed to parse explain response", e)
                            actionListener.onFailure(e)
                            return
                        }

                        val metadataIds = idsToExplain.values.mapNotNull { it?.metadataID }
                        val metadataSearchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX)
                            .source(SearchSourceBuilder().size(MAX_HITS).query(IdsQueryBuilder().addIds(*metadataIds.toTypedArray())))
                        client.search(
                            metadataSearchRequest,
                            object : ActionListener<SearchResponse> {
                                override fun onResponse(response: SearchResponse) {
                                    try {
                                        response.hits.hits.forEach {
                                            val metadata = contentParser(it.sourceRef)
                                                .parseWithType(it.id, it.seqNo, it.primaryTerm, RollupMetadata.Companion::parse)
                                            idsToExplain.computeIfPresent(metadata.rollupID) { _,
                                                explainRollup ->
                                                explainRollup.copy(metadata = metadata)
                                            }
                                        }
                                        actionListener.onResponse(ExplainRollupResponse(idsToExplain.toMap()))
                                    } catch (e: Exception) {
                                        log.error("Failed to parse rollup metadata", e)
                                        actionListener.onFailure(e)
                                        return
                                    }
                                }

                                override fun onFailure(e: Exception) {
                                    log.error("Failed to search rollup metadata", e)
                                    when (e) {
                                        is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                                        else -> actionListener.onFailure(e)
                                    }
                                }
                            }
                        )
                    }

                    override fun onFailure(e: Exception) {
                        log.error("Failed to search for rollups", e)
                        when (e) {
                            is ResourceNotFoundException -> {
                                val nonWildcardIds = ids.filter { !it.contains("*") }.map { it to null }.toMap(mutableMapOf())
                                actionListener.onResponse(ExplainRollupResponse(nonWildcardIds))
                            }
                            is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                            else -> actionListener.onFailure(e)
                        }
                    }
                }
            )
        }
    }
}
