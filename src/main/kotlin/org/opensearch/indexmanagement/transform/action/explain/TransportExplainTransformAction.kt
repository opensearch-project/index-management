/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.explain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.IdsQueryBuilder
import org.opensearch.index.query.WildcardQueryBuilder
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.transform.model.ExplainTransform
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.addUserFilter
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

@Suppress("LongParameterList")
class TransportExplainTransformAction
@Inject
constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val pluginClient: PluginClient,
) : HandledTransportAction<ExplainTransformRequest, ExplainTransformResponse>(
    ExplainTransformAction.NAME, transportService, actionFilters, ::ExplainTransformRequest,
) {
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator", "NestedBlockDepth", "LongMethod")
    override fun doExecute(task: Task, request: ExplainTransformRequest, actionListener: ActionListener<ExplainTransformResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
            )}",
        )
        val ids = request.transformIDs
        // Instantiate concrete ids to metadata map by removing wildcard matches
        val idsToExplain: MutableMap<String, ExplainTransform?> =
            ids.filter { !it.contains("*") }
                .map { it to null }.toMap(mutableMapOf())
        val failedToExplain: MutableMap<String, String> = mutableMapOf()
        val queryBuilder =
            BoolQueryBuilder().minimumShouldMatch(1).apply {
                ids.forEach {
                    this.should(WildcardQueryBuilder("${ Transform.TRANSFORM_TYPE}.${Transform.TRANSFORM_ID_FIELD}.keyword", "*$it*"))
                }
            }
        val user = buildUser(client.threadPool().threadContext)
        addUserFilter(user, queryBuilder, filterByEnabled, "transform.user")

        val searchRequest = SearchRequest(INDEX_MANAGEMENT_INDEX).source(SearchSourceBuilder().seqNoAndPrimaryTerm(true).query(queryBuilder))

        pluginClient.search(
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
                    val metadataSearchRequest =
                        SearchRequest(INDEX_MANAGEMENT_INDEX)
                            .source(SearchSourceBuilder().query(IdsQueryBuilder().addIds(*metadataIds.toTypedArray())))
                    pluginClient.search(
                        metadataSearchRequest,
                        object : ActionListener<SearchResponse> {
                            override fun onResponse(response: SearchResponse) {
                                CoroutineScope(Dispatchers.IO).launch {
                                    response.hits.hits.forEach {
                                        try {
                                            val metadata =
                                                contentParser(it.sourceRef)
                                                    .parseWithType(it.id, it.seqNo, it.primaryTerm, TransformMetadata.Companion::parse)

                                            val transform = metadataIdToTransform[metadata.id]
                                            // Only add continuous stats for continuous transforms which have not failed
                                            if (transform?.continuous == true && metadata.status != TransformMetadata.Status.FAILED) {
                                                addContinuousStats(transform, metadata)
                                            } else {
                                                idsToExplain.computeIfPresent(metadata.transformId) { _, explainTransform ->
                                                    // Don't provide shardIDToGlobalCheckpoint for a failed or non-continuous transform
                                                    explainTransform.copy(metadata = metadata.copy(shardIDToGlobalCheckpoint = null))
                                                }
                                            }
                                        } catch (e: Exception) {
                                            log.error("Failed to parse transform [${it.id}] metadata", e)
                                            idsToExplain.remove(it.id)
                                            failedToExplain[it.id] =
                                                "Failed to parse transform metadata - ${e.message}"
                                        }
                                    }
                                    actionListener.onResponse(ExplainTransformResponse(idsToExplain.toMap(), failedToExplain))
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

                            private suspend fun addContinuousStats(transform: Transform, metadata: TransformMetadata) {
                                val continuousStats = transform.getContinuousStats(client, metadata)
                                if (continuousStats == null) {
                                    log.error("Failed to get continuous transform stats for transform [${transform.id}]")
                                    idsToExplain.remove(transform.id)
                                    failedToExplain[transform.id] =
                                        "Failed to get continuous transform stats"
                                } else {
                                    idsToExplain.computeIfPresent(metadata.transformId) { _, explainTransform ->
                                        explainTransform.copy(
                                            metadata =
                                            metadata.copy(
                                                shardIDToGlobalCheckpoint = null,
                                                continuousStats = continuousStats,
                                            ),
                                        )
                                    }
                                }
                            }
                        },
                    )
                }

                override fun onFailure(e: Exception) {
                    log.error("Failed to search for transforms", e)
                    when (e) {
                        is ResourceNotFoundException -> {
                            val failureReason = "Failed to search transform metadata"
                            val nonWildcardIds = ids.filter { !it.contains("*") }.map { it to failureReason }.toMap(mutableMapOf())
                            actionListener.onResponse(ExplainTransformResponse(mapOf(), nonWildcardIds))
                        }
                        is RemoteTransportException -> actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as java.lang.Exception)
                        else -> actionListener.onFailure(e)
                    }
                }
            },
        )
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser = XContentHelper.createParser(
        xContentRegistry,
        LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON,
    )
}
