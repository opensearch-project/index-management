/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.Index
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.ExistsQueryBuilder
import org.opensearch.index.query.QueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.query.RangeQueryBuilder
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.transform.exceptions.TransformSearchServiceException
import org.opensearch.indexmanagement.transform.model.BucketSearchResult
import org.opensearch.indexmanagement.transform.model.ShardNewDocuments
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformSearchResult
import org.opensearch.indexmanagement.transform.model.TransformStats
import org.opensearch.indexmanagement.transform.settings.TransformSettings.Companion.TRANSFORM_JOB_SEARCH_BACKOFF_COUNT
import org.opensearch.indexmanagement.transform.settings.TransformSettings.Companion.TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS
import org.opensearch.indexmanagement.util.IndexUtils.Companion.ODFE_MAGIC_NULL
import org.opensearch.indexmanagement.util.IndexUtils.Companion.hashToFixedSize
import org.opensearch.rest.RestStatus
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

@Suppress("ThrowsCount", "TooManyFunctions")
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

    @Suppress("RethrowCaughtException")
    suspend fun getShardsGlobalCheckpoint(index: String): Map<ShardId, Long> {
        try {
            var retryAttempt = 1
            // Retry on standard retry fail statuses plus NOT_FOUND in case a shard routing entry isn't ready yet
            val searchResponse: IndicesStatsResponse = backoffPolicy.retry(logger, listOf(RestStatus.NOT_FOUND)) {
                val request = IndicesStatsRequest().indices(index).clear()
                if (retryAttempt > 1) {
                    logger.debug(getShardsRetryMessage(retryAttempt))
                }
                retryAttempt++
                client.suspendUntil { execute(IndicesStatsAction.INSTANCE, request, it) }
            }
            if (searchResponse.status == RestStatus.OK) {
                return convertIndicesStatsResponse(searchResponse)
            }
            throw TransformSearchServiceException("$getShardsErrorMessage - ${searchResponse.status}")
        } catch (e: TransformSearchServiceException) {
            throw e
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw TransformSearchServiceException(getShardsErrorMessage, unwrappedException)
        } catch (e: OpenSearchSecurityException) {
            throw TransformSearchServiceException("$getShardsErrorMessage - missing required index permissions: ${e.localizedMessage}", e)
        } catch (e: Exception) {
            throw TransformSearchServiceException(getShardsErrorMessage, e)
        }
    }

    @Suppress("RethrowCaughtException")
    suspend fun getShardLevelModifiedBuckets(transform: Transform, afterKey: Map<String, Any>?, currentShard: ShardNewDocuments): BucketSearchResult {
        try {
            var retryAttempt = 0
            val searchResponse = backoffPolicy.retry(logger) {
                val pageSizeDecay = 2f.pow(retryAttempt++)
                client.suspendUntil { listener: ActionListener<SearchResponse> ->
                    val pageSize = max(1, transform.pageSize.div(pageSizeDecay.toInt()))
                    if (retryAttempt > 1) {
                        logger.debug(
                            "Attempt [${retryAttempt - 1}] to get modified buckets for transform [${transform.id}]. Attempting " +
                                "again with reduced page size [$pageSize]"
                        )
                    }
                    val request = getShardLevelBucketsSearchRequest(transform, afterKey, pageSize, currentShard)
                    search(request, listener)
                }
            }
            return convertBucketSearchResponse(transform, searchResponse)
        } catch (e: TransformSearchServiceException) {
            throw e
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw TransformSearchServiceException(modifiedBucketsErrorMessage, unwrappedException)
        } catch (e: OpenSearchSecurityException) {
            throw TransformSearchServiceException("$modifiedBucketsErrorMessage - missing required index permissions: ${e.localizedMessage}", e)
        } catch (e: Exception) {
            throw TransformSearchServiceException(modifiedBucketsErrorMessage, e)
        }
    }

    @Suppress("RethrowCaughtException")
    suspend fun executeCompositeSearch(
        transform: Transform,
        afterKey: Map<String, Any>? = null,
        modifiedBuckets: MutableSet<Map<String, Any>>? = null
    ): TransformSearchResult {
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
                    val request = getSearchServiceRequest(transform, afterKey, pageSize, modifiedBuckets)
                    search(request, listener)
                }
            }
            return convertResponse(transform, searchResponse, modifiedBuckets = modifiedBuckets)
        } catch (e: TransformSearchServiceException) {
            throw e
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw TransformSearchServiceException(failedSearchErrorMessage, unwrappedException)
        } catch (e: OpenSearchSecurityException) {
            throw TransformSearchServiceException("$failedSearchErrorMessage - missing required index permissions: ${e.localizedMessage}", e)
        } catch (e: Exception) {
            throw TransformSearchServiceException(failedSearchErrorMessage, e)
        }
    }

    companion object {
        const val failedSearchErrorMessage = "Failed to search data in source indices"
        const val modifiedBucketsErrorMessage = "Failed to get the modified buckets in source indices"
        const val getShardsErrorMessage = "Failed to get the shards in the source indices"
        private fun getShardsRetryMessage(attemptNumber: Int) = "Attempt [$attemptNumber] to get shard global checkpoint numbers"
        private fun noTransformGroupErrorMessage(bucketField: String) = "Failed to find a transform group matching the bucket field [$bucketField]"

        fun getSearchServiceRequest(
            transform: Transform,
            afterKey: Map<String, Any>? = null,
            pageSize: Int,
            modifiedBuckets: MutableSet<Map<String, Any>>? = null
        ): SearchRequest {
            val sources = mutableListOf<CompositeValuesSourceBuilder<*>>()
            transform.groups.forEach { group -> sources.add(group.toSourceBuilder().missingBucket(true)) }
            val aggregationBuilder = CompositeAggregationBuilder(transform.id, sources)
                .size(pageSize)
                .subAggregations(transform.aggregations)
                .apply { afterKey?.let { this.aggregateAfter(it) } }
            val query = if (modifiedBuckets == null) {
                transform.dataSelectionQuery
            } else {
                getQueryWithModifiedBuckets(transform.dataSelectionQuery, modifiedBuckets, transform.groups)
            }
            return getSearchServiceRequest(transform.sourceIndex, query, aggregationBuilder)
        }

        private fun getQueryWithModifiedBuckets(
            originalQuery: QueryBuilder,
            modifiedBuckets: MutableSet<Map<String, Any>>,
            groups: List<Dimension>
        ): QueryBuilder {
            val query: BoolQueryBuilder = QueryBuilders.boolQuery().must(originalQuery).minimumShouldMatch(1)
            modifiedBuckets.forEach { bucket ->
                val bucketQuery: BoolQueryBuilder = QueryBuilders.boolQuery()
                bucket.forEach { group ->
                    // There should be a transform grouping for each bucket key, if not then throw an error
                    val transformGroup = groups.find { it.targetField == group.key }
                        ?: throw TransformSearchServiceException(noTransformGroupErrorMessage(group.key))
                    if (group.value as Any? == null) {
                        val subQuery = ExistsQueryBuilder(transformGroup.sourceField)
                        bucketQuery.mustNot(subQuery)
                    } else {
                        val subQuery = transformGroup.toBucketQuery(group.value)
                        bucketQuery.filter(subQuery)
                    }
                }
                query.should(bucketQuery)
            }
            return query
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

        fun getShardLevelBucketsSearchRequest(
            transform: Transform,
            afterKey: Map<String, Any>? = null,
            pageSize: Int,
            currentShard: ShardNewDocuments
        ): SearchRequest {
            val rangeQuery = getSeqNoRangeQuery(currentShard.from, currentShard.to)
            val query = QueryBuilders.boolQuery().filter(rangeQuery).must(transform.dataSelectionQuery)
            val sources = transform.groups.map { it.toSourceBuilder().missingBucket(true) }
            val aggregationBuilder = CompositeAggregationBuilder(transform.id, sources)
                .size(pageSize)
                .apply { afterKey?.let { this.aggregateAfter(it) } }
            return getSearchServiceRequest(currentShard.shardId.indexName, query, aggregationBuilder)
                .preference("_shards:" + currentShard.shardId.id.toString())
        }

        private fun getSeqNoRangeQuery(from: Long?, to: Long): RangeQueryBuilder {
            val rangeQuery = RangeQueryBuilder("_seq_no")
            // If to or from is < 0 then the step to get the global checkpoint number failed, and we proceed without bounding the sequence number
            if (to >= 0) rangeQuery.to(to, true)
            if (from != null && from >= 0) rangeQuery.from(from, false)
            return rangeQuery
        }

        fun convertResponse(
            transform: Transform,
            searchResponse: SearchResponse,
            waterMarkDocuments: Boolean = true,
            modifiedBuckets: MutableSet<Map<String, Any>>? = null
        ): TransformSearchResult {
            val aggs = searchResponse.aggregations.get(transform.id) as CompositeAggregation
            val buckets = if (modifiedBuckets != null) aggs.buckets.filter { modifiedBuckets.contains(it.key) } else aggs.buckets
            val documentsProcessed = buckets.fold(0L) { sum, it -> sum + it.docCount }
            val pagesProcessed = 1L
            val searchTime = searchResponse.took.millis
            val stats = TransformStats(pagesProcessed, documentsProcessed, 0, 0, searchTime)
            val afterKey = aggs.afterKey()
            val docsToIndex = mutableListOf<IndexRequest>()
            buckets.forEach { aggregatedBucket ->
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

        // Gathers and returns from the bucket search response the modified buckets from the query, the afterkey, and the search time
        private fun convertBucketSearchResponse(
            transform: Transform,
            searchResponse: SearchResponse
        ): BucketSearchResult {
            val aggs = searchResponse.aggregations.get(transform.id) as CompositeAggregation
            val modifiedBuckets = aggs.buckets.map { it.key }.toMutableSet()
            return BucketSearchResult(modifiedBuckets, aggs.afterKey(), searchResponse.took.millis)
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

        fun convertIndicesStatsResponse(response: IndicesStatsResponse): Map<ShardId, Long> {
            val shardStats = HashMap<ShardId, Long>()
            val shardsToSearch = response.shards.filter { it.shardRouting.primary() && it.shardRouting.active() }
            for (shard in shardsToSearch) {
                val shardId = shard.shardRouting.shardId()
                // Remove uuid as it isn't streamed, so it would break our hashing. We aren't using it anyways
                val shardIDNoUUID = ShardId(Index(shardId.index.name, IndexMetadata.INDEX_UUID_NA_VALUE), shardId.id)
                // If it is null, we will still run the transform, but without bounding the sequence number
                shardStats[shardIDNoUUID] = shard.seqNoStats?.globalCheckpoint ?: SequenceNumbers.UNASSIGNED_SEQ_NO
            }
            return shardStats
        }
    }
}
