/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.explain

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.IdsQueryBuilder
import org.opensearch.index.query.WildcardQueryBuilder
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.transform.TransformSearchService
import org.opensearch.indexmanagement.transform.model.ContinuousTransformStats
import org.opensearch.indexmanagement.transform.model.ExplainTransform
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.addUserFilter
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.TransportService
import java.lang.Long.max

class TransportExplainTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ExplainTransformRequest, ExplainTransformResponse>(
    ExplainTransformAction.NAME, transportService, actionFilters, ::ExplainTransformRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator")
    override fun doExecute(task: Task, request: ExplainTransformRequest, actionListener: ActionListener<ExplainTransformResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val ids = request.transformIDs
        // Instantiate concrete ids to metadata map by removing wildcard matches
        val idsToExplain: MutableMap<String, ExplainTransform?> = ids.filter { !it.contains("*") }
            .map { it to null }.toMap(mutableMapOf())
        val queryBuilder = BoolQueryBuilder().minimumShouldMatch(1).apply {
            ids.forEach {
                this.should(WildcardQueryBuilder("${ Transform.TRANSFORM_TYPE}.${Transform.TRANSFORM_ID_FIELD}.keyword", "*$it*"))
            }
        }
        val user = buildUser(client.threadPool().threadContext)
        addUserFilter(user, queryBuilder, filterByEnabled, "transform.user")

        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().seqNoAndPrimaryTerm(true).query(queryBuilder))

        client.threadPool().threadContext.stashContext().use {
            client.search(
                searchRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(response: SearchResponse) {
                        val metadataIdToTransform: MutableMap<String, Transform> = HashMap()
                        try {
                            response.hits.hits.forEach {
                                val transform = contentParser(it.sourceRef).parseWithType(it.id, it.seqNo, it.primaryTerm, Transform.Companion::parse)
                                idsToExplain[transform.id] = ExplainTransform(metadataID = transform.metadataId)
                                if (transform.metadataId != null) metadataIdToTransform[transform.metadataId] = transform
                            }
                        } catch (e: Exception) {
                            log.error("Failed to parse explain response", e)
                            actionListener.onFailure(e)
                            return
                        }

                        val metadataIds = idsToExplain.values.mapNotNull { it?.metadataID }
                        val metadataSearchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX)
                            .source(SearchSourceBuilder().query(IdsQueryBuilder().addIds(*metadataIds.toTypedArray())))
                        client.search(
                            metadataSearchRequest,
                            object : ActionListener<SearchResponse> {
                                override fun onResponse(response: SearchResponse) {
                                    try {
                                        response.hits.hits.forEach {
                                            val metadata = contentParser(it.sourceRef)
                                                .parseWithType(it.id, it.seqNo, it.primaryTerm, TransformMetadata.Companion::parse)
                                            idsToExplain.computeIfPresent(metadata.transformId) { _, explainTransform ->
                                                explainTransform.copy(metadata = metadata)
                                            }
                                        }
                                        if (metadataIdToTransform.values.any { it.continuous }) {
                                            val transformsToSearch = metadataIdToTransform.values.filter { it.continuous }.iterator()
                                            addContinuousStats(transformsToSearch)
                                        } else {
                                            actionListener.onResponse(ExplainTransformResponse(idsToExplain.toMap()))
                                        }
                                    } catch (e: Exception) {
                                        log.error("Failed to parse transform metadata", e)
                                        actionListener.onFailure(e)
                                        return
                                    }
                                }

                                override fun onFailure(e: Exception) {
                                    log.error("Failed to search transform metadata", e)
                                    when (e) {
                                        is RemoteTransportException ->
                                            actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as java.lang.Exception)
                                        else -> actionListener.onFailure(e)
                                    }
                                }

                                // Recursively adds continuous stats for the next transform in the iterator
                                private fun addContinuousStats(transformsToSearch: Iterator<Transform>) {
                                    if (!transformsToSearch.hasNext()) {
                                        actionListener.onResponse(ExplainTransformResponse(idsToExplain.toMap()))
                                    } else {
                                        val currTransform = transformsToSearch.next()
                                        val indicesStatsRequest =
                                            IndicesStatsRequest().indices(currTransform.sourceIndex).clear()
                                        // If this call fails then documentsBehind will just be null
                                        client.execute(
                                            IndicesStatsAction.INSTANCE,
                                            indicesStatsRequest,
                                            object : ActionListener<IndicesStatsResponse> {
                                                override fun onResponse(response: IndicesStatsResponse) {
                                                    val shardIDsToGlobalCheckpoint =
                                                        if (response.status == RestStatus.OK) {
                                                            TransformSearchService.convertIndicesStatsResponse(response)
                                                        } else null
                                                    val newMetadata = idsToExplain[currTransform.id]!!.metadata?.copy(
                                                        shardIDToGlobalCheckpoint = null,
                                                        continuousStats = ContinuousTransformStats(
                                                            idsToExplain[currTransform.id]!!.metadata!!.continuousStats?.lastTimestamp,
                                                            getDocumentsBehind(
                                                                idsToExplain[currTransform.id]!!.metadata!!.shardIDToGlobalCheckpoint,
                                                                shardIDsToGlobalCheckpoint
                                                            )
                                                        )
                                                    )
                                                    idsToExplain[currTransform.id] =
                                                        idsToExplain[currTransform.id]!!.copy(metadata = newMetadata)
                                                    addContinuousStats(transformsToSearch)
                                                }

                                                override fun onFailure(e: Exception) {
                                                    log.error("Failed to parse indices stats response", e)
                                                    actionListener.onFailure(e)
                                                    return
                                                }

                                                private fun getDocumentsBehind(
                                                    oldShardIDsToGlobalCheckpoint: Map<ShardId, Long>?,
                                                    newShardIDsToGlobalCheckpoint: Map<ShardId, Long>?
                                                ): MutableMap<String, Long> {
                                                    val documentsBehind: MutableMap<String, Long> = HashMap()
                                                    if (newShardIDsToGlobalCheckpoint == null) {
                                                        return documentsBehind
                                                    }
                                                    newShardIDsToGlobalCheckpoint.forEach { (shardID, globalCheckpoint) ->
                                                        val indexName = shardID.indexName
                                                        val newGlobalCheckpoint = max(0, globalCheckpoint)
                                                        // global checkpoint may be -1 or -2 if not initialized, just set to 0 in those cases
                                                        val oldGlobalCheckpoint =
                                                            max(0, oldShardIDsToGlobalCheckpoint?.get(shardID) ?: 0)
                                                        val localDocsBehind = newGlobalCheckpoint - oldGlobalCheckpoint
                                                        documentsBehind[indexName] =
                                                            (documentsBehind[indexName] ?: 0) + localDocsBehind
                                                    }

                                                    return documentsBehind
                                                }
                                            }
                                        )
                                    }
                                }
                            }
                        )
                    }

                    override fun onFailure(e: Exception) {
                        log.error("Failed to search for transforms", e)
                        when (e) {
                            is ResourceNotFoundException -> {
                                val nonWildcardIds = ids.filter { !it.contains("*") }.map { it to null }.toMap(mutableMapOf())
                                actionListener.onResponse(ExplainTransformResponse(nonWildcardIds))
                            }
                            is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as java.lang.Exception)
                            else -> actionListener.onFailure(e)
                        }
                    }
                }
            )
        }
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(
            xContentRegistry,
            LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON
        )
    }
}
