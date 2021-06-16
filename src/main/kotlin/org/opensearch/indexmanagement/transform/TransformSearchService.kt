/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.transform

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.QueryBuilder
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.transform.exceptions.TransformSearchServiceException
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformSearchResult
import org.opensearch.indexmanagement.transform.model.TransformStats
import org.opensearch.indexmanagement.transform.settings.TransformSettings.Companion.TRANSFORM_JOB_SEARCH_BACKOFF_COUNT
import org.opensearch.indexmanagement.transform.settings.TransformSettings.Companion.TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS
import org.opensearch.indexmanagement.util.IndexUtils.Companion.ODFE_MAGIC_NULL
import org.opensearch.indexmanagement.util.IndexUtils.Companion.hashToFixedSize
import org.opensearch.search.aggregations.Aggregation
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder
import org.opensearch.search.aggregations.metrics.InternalAvg
import org.opensearch.search.aggregations.metrics.InternalMax
import org.opensearch.search.aggregations.metrics.InternalMin
import org.opensearch.search.aggregations.metrics.InternalSum
import org.opensearch.search.aggregations.metrics.InternalValueCount
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation
import org.opensearch.search.aggregations.metrics.Percentiles
import org.opensearch.search.aggregations.metrics.ScriptedMetric
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.RemoteTransportException
import kotlin.math.max
import kotlin.math.pow

@Suppress("ThrowsCount")
class TransformSearchService(
    val settings: Settings,
    val clusterService: ClusterService,
    private val client: Client
) {

    private var logger = LogManager.getLogger(javaClass)

    @Volatile private var backoffPolicy =
        BackoffPolicy.constantBackoff(TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS.get(settings), TRANSFORM_JOB_SEARCH_BACKOFF_COUNT.get(settings))

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS, TRANSFORM_JOB_SEARCH_BACKOFF_COUNT) {
            millis, count ->
            backoffPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    suspend fun executeCompositeSearch(transform: Transform, afterKey: Map<String, Any>? = null): TransformSearchResult {
        val errorMessage = "Failed to search data in source indices in transform job ${transform.id}"
        try {
            var retryAttempt = 0
            val searchResponse = backoffPolicy.retry(logger) {
                // TODO: Should we store the value of the past successful page size (?)
                val pageSizeDecay = 2f.pow(retryAttempt++)
                client.suspendUntil { listener: ActionListener<SearchResponse> ->
                    val pageSize = max(1, transform.pageSize.div(pageSizeDecay.toInt()))
                    if (retryAttempt > 1) {
                        logger.debug(
                            "Attempt [${retryAttempt - 1}] of composite search failed for transform [${transform.id}]. Attempting " +
                                "again with reduced page size [$pageSize]"
                        )
                    }
                    val request = getSearchServiceRequest(transform, afterKey, pageSize)
                    search(request, listener)
                }
            }
            return convertResponse(transform, searchResponse)
        } catch (e: TransformSearchServiceException) {
            logger.error(errorMessage)
            throw e
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.error(errorMessage, unwrappedException)
            throw TransformSearchServiceException(errorMessage, unwrappedException)
        } catch (e: Exception) {
            logger.error(errorMessage, e)
            throw TransformSearchServiceException(errorMessage, e)
        }
    }

    companion object {
        fun getSearchServiceRequest(transform: Transform, afterKey: Map<String, Any>? = null, pageSize: Int): SearchRequest {
            val sources = mutableListOf<CompositeValuesSourceBuilder<*>>()
            transform.groups.forEach { group -> sources.add(group.toSourceBuilder()) }
            val aggregationBuilder = CompositeAggregationBuilder(transform.id, sources)
                .size(pageSize)
                .subAggregations(transform.aggregations)
                .apply { afterKey?.let { this.aggregateAfter(it) } }
            return getSearchServiceRequest(transform.sourceIndex, transform.dataSelectionQuery, aggregationBuilder)
        }

        private fun getSearchServiceRequest(index: String, query: QueryBuilder, aggregationBuilder: CompositeAggregationBuilder): SearchRequest {
            val searchSourceBuilder = SearchSourceBuilder()
                .trackTotalHits(false)
                .size(0)
                .aggregation(aggregationBuilder)
                .query(query)
            return SearchRequest(index)
                .source(searchSourceBuilder)
                .allowPartialSearchResults(false)
        }

        fun convertResponse(transform: Transform, searchResponse: SearchResponse, waterMarkDocuments: Boolean = true): TransformSearchResult {
            val aggs = searchResponse.aggregations.get(transform.id) as CompositeAggregation
            val documentsProcessed = aggs.buckets.fold(0L) { sum, it -> sum + it.docCount }
            val pagesProcessed = 1L
            val searchTime = searchResponse.took.millis
            val stats = TransformStats(pagesProcessed, documentsProcessed, 0, 0, searchTime)
            val afterKey = aggs.afterKey()
            val docsToIndex = mutableListOf<IndexRequest>()
            aggs.buckets.forEach { aggregatedBucket ->
                val id = transform.id + "#" + aggregatedBucket.key.entries.joinToString(":") { bucket -> bucket.value?.toString() ?: ODFE_MAGIC_NULL }
                val hashedId = hashToFixedSize(id)

                val document = transform.convertToDoc(aggregatedBucket.docCount, waterMarkDocuments)
                aggregatedBucket.key.entries.forEach { bucket -> document[bucket.key] = bucket.value }
                aggregatedBucket.aggregations.forEach { aggregation -> document[aggregation.name] = getAggregationValue(aggregation) }

                val indexRequest = IndexRequest(transform.targetIndex)
                    .id(hashedId)
                    .source(document, XContentType.JSON)
                docsToIndex.add(indexRequest)
            }

            return TransformSearchResult(stats, docsToIndex, afterKey)
        }

        private fun getAggregationValue(aggregation: Aggregation): Any {
            return when (aggregation) {
                is InternalSum, is InternalMin, is InternalMax, is InternalAvg, is InternalValueCount -> {
                    val agg = aggregation as NumericMetricsAggregation.SingleValue
                    agg.value()
                }
                is Percentiles -> {
                    val percentiles = mutableMapOf<String, Double>()
                    aggregation.forEach { percentile ->
                        percentiles[percentile.percent.toString()] = percentile.value
                    }
                    percentiles
                }
                is ScriptedMetric -> {
                    aggregation.aggregation()
                }
                else -> throw TransformSearchServiceException(
                    "Found aggregation [${aggregation.name}] of type [${aggregation.type}] in composite result that is not currently supported"
                )
            }
        }
    }
}
