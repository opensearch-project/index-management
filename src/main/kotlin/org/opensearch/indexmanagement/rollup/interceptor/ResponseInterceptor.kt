/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.util.getRollupJobs
import org.opensearch.indexmanagement.rollup.util.isRollupIndex
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.QueryFetchSearchResult
import org.opensearch.search.internal.ShardSearchRequest
import org.opensearch.search.query.QuerySearchResult
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.transport.TransportInterceptor
import org.opensearch.transport.TransportResponse
import org.opensearch.transport.TransportRequest
import org.opensearch.transport.Transport
import org.opensearch.transport.TransportRequestOptions
import org.opensearch.transport.TransportResponseHandler
import org.opensearch.transport.TransportException
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.CountDownLatch

class ResponseInterceptor(
    val clusterService: ClusterService,
    val settings: Settings,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    val client: Client
) : TransportInterceptor {
    private val logger = LogManager.getLogger(javaClass)

    override fun interceptSender(sender: TransportInterceptor.AsyncSender): TransportInterceptor.AsyncSender {
        return CustomAsyncSender(sender)
    }

    private inner class CustomAsyncSender(private val originalSender: TransportInterceptor.AsyncSender) : TransportInterceptor.AsyncSender {
        // Logic for overlap

        override fun <T : TransportResponse?> sendRequest(
            connection: Transport.Connection?,
            action: String?,
            request: TransportRequest?,
            options: TransportRequestOptions?,
            handler: TransportResponseHandler<T>?
        ) {
            val interceptedHandler = CustomResponseHandler(handler)

            originalSender.sendRequest(connection, action, request, options, interceptedHandler)
        }
    }

    private inner class CustomResponseHandler<T : TransportResponse?>(
        private val originalHandler: TransportResponseHandler<T>?
    ) : TransportResponseHandler<T> {
        override fun read(inStream: StreamInput?): T {
            val response = originalHandler?.read(inStream)
            // Modify the response if necessary
            return response!!
        }
        fun isRewrittenInterceptorRequest(response: QuerySearchResult): Boolean {
            val currentAggregations = response.aggregations().expand()
            for (agg in currentAggregations) {
                if (agg.name == "interceptor_interval_data") {
                    return true
                }
            }
            return false
        }
        fun getRollupJob(response: QuerySearchResult): Rollup? {
            val originalRequest = response.shardSearchRequest!!
            val indices = originalRequest.indices().map { it.toString() }.toTypedArray()
            val allIndices = indexNameExpressionResolver
                .concreteIndexNames(clusterService.state(), originalRequest.indicesOptions(), *indices)
            for (index in allIndices) {
                if (isRollupIndex(index, clusterService.state())) {
                    return clusterService.state().metadata.index(index).getRollupJobs()?.get(0)!!
                }
            }
            return null
        }
        fun getRollupAndLiveIndices(request: ShardSearchRequest): Pair<Array<String>, Array<String>> {
            val liveIndices = mutableListOf<String>()
            val rollupIndices = mutableListOf<String>()
            val indices = request.indices().map { it.toString() }.toTypedArray()
            val concreteIndices = indexNameExpressionResolver
                .concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
            for (indexName in concreteIndices) {
                if (isRollupIndex(indexName, clusterService.state())) {
                    rollupIndices.add(indexName)
                } else {
                    liveIndices.add(indexName)
                }
            }
            return Pair(rollupIndices.toTypedArray(), liveIndices.toTypedArray())
        }
        fun convertEpochMillisToDateString(epochMillis: Long): String {
            val pattern = "yyyy-MM-dd HH:mm:ss"
            val dateFormat = SimpleDateFormat(pattern)
            val date = Date(epochMillis)
            val dateString = dateFormat.format(date)
            return dateString
        }
        fun convertDateStringToEpochMillis(dateString: String): Long {
            val pattern = "yyyy-MM-dd HH:mm:ss"
            val dateFormat = SimpleDateFormat(pattern)

            try {
                val date = dateFormat.parse(dateString)
                return (date.time)
            } catch (e: Exception) {
                println("Error parsing date: ${e.message}")
            }
            return 0L
        }
        fun convertFixedIntervalStringToMs(fixedInterval: String): Long {
            // Possible types are ms, s, m, h, d
            val regex = """(\d+)([a-zA-Z]+)""".toRegex()
            val matchResult = regex.find(fixedInterval)
                ?: throw IllegalArgumentException("Invalid interval format: $fixedInterval")

            val numericValue = matchResult.groupValues[1].toLong()
            val intervalType = matchResult.groupValues[2]

            val milliseconds = when (intervalType) {
                "ms" -> numericValue
                "s" -> numericValue * 1000L
                "m" -> numericValue * 60 * 1000L
                "h" -> numericValue * 60 * 60 * 1000L
                "d" -> numericValue * 24 * 60 * 60 * 1000L
                "w" -> numericValue * 7 * 24 * 60 * 60 * 1000L
                else -> throw IllegalArgumentException("Unsupported interval type: $intervalType")
            }

            return milliseconds
        }

//         Returns Pair(startRange: Long, endRange: Long)
//         Note startRange is inclusive and endRange is exclusive, they are longs becuase the type is epoch milliseconds
        fun findOverlap(response: QuerySearchResult): Pair<Long, Long> {
            val job: Rollup = getRollupJob(response)!! // maybe throw a try catch later
            var dateSourceField: String = ""
            var dateTargetField: String = ""
            var rollupInterval: String? = ""
            for (dim in job.dimensions) {
                if (dim is DateHistogram) {
                    dateSourceField = dim.sourceField
                    dateTargetField = dim.targetField
                    rollupInterval = dim.fixedInterval
                    break
                }
            }
            // Keep existing query and add 3 fake match alls to avoid infinite loop
            val request: ShardSearchRequest = response.shardSearchRequest!!
            val oldQuery = request.source().query()
//            val fakeQuery = QueryBuilders.boolQuery()
//                .must(oldQuery ?: QueryBuilders.matchAllQuery())
//                .must(QueryBuilders.matchAllQuery())
//                .must(QueryBuilders.matchAllQuery())
//                .must(QueryBuilders.matchAllQuery())
            // TODO scale this for multiple indices!!!!
            val (rollupIndices, liveIndices) = getRollupAndLiveIndices(request)
            val rollupIndex = rollupIndices[0]
            val liveIndex = liveIndices[0]
            // Build search request to find the minimum date in the live data index
            var sort = SortBuilders.fieldSort(dateSourceField).order(SortOrder.ASC)
            var searchSourceBuilder = SearchSourceBuilder()
                .sort(sort)
                .size(1)
            val minLiveDateRequest = SearchRequest()
                .indices(liveIndex)
                .source(searchSourceBuilder)

            var minLiveDateResponse: SearchResponse? = null
            var latch = CountDownLatch(1)
            client.search(
                minLiveDateRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(searchResponse: SearchResponse) {
                        minLiveDateResponse = searchResponse
                        latch.countDown()
                    }

                    override fun onFailure(e: Exception) {
                        logger.error("ronsax minLiveDate request failed ", e)
                        latch.countDown()
                    }
                }
            )
            latch.await()

            // Build search request to find the maximum date on the rolled data index
            sort = SortBuilders.fieldSort("$dateTargetField.date_histogram").order(SortOrder.DESC)
            searchSourceBuilder = SearchSourceBuilder()
                .sort(sort)
                .query(oldQuery)
                .size(1)
            // Need to avoid infinite interceptor loop
            val maxRolledDateRequest = SearchRequest()
                .indices(rollupIndex)
                .source(searchSourceBuilder)
            var maxRolledDateResponse: SearchResponse? = null
            latch = CountDownLatch(1)
            client.search(
                maxRolledDateRequest,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(searchResponse: SearchResponse) {
                        maxRolledDateResponse = searchResponse
                        latch.countDown()
                    }

                    override fun onFailure(e: Exception) {
                        logger.error("ronsax maxLiveDate request failed ", e)
                        latch.countDown()
                    }
                }
            )
            latch.await()

            if (minLiveDateResponse != null && maxRolledDateResponse != null) {
                // Rollup data ends at maxRolledDate + fixedInterval
                val maxRolledDate: Long = maxRolledDateResponse!!.hits.hits[0].sourceAsMap.get("$dateTargetField.date_histogram") as Long // broken for rollup index
                val rollupDataEndPoint = maxRolledDate + convertFixedIntervalStringToMs(fixedInterval = rollupInterval!!)
                val minLiveDate = minLiveDateResponse!!.hits.hits[0].sourceAsMap.get("$dateSourceField") as String // broken for rollup index
                val liveDataStartPoint = convertDateStringToEpochMillis(minLiveDate)
                if (liveDataStartPoint <= rollupDataEndPoint) {
                    // Find intersection timestamp
                    val intersectionTime = maxRolledDate
                    val shardRequestIndex = request.shardId().indexName
                    if (shardRequestIndex == liveIndex) {
                        // Start at intersection timestamp, end at inf
                        return Pair(intersectionTime, Long.MAX_VALUE)
                    } else if (shardRequestIndex == rollupIndex) {
                        // Start at 0, end at intersection time
                        return Pair(0L, intersectionTime)
                    }
                }
            } else {
                logger.error("Not able to make client calls in response interceptor")
            }
            // No overlap so start and end include everything
            return Pair(0L, Long.MAX_VALUE)
        }

//         Returns a new InternalAggregations that contains a merged aggregation(s) with the overlapping data removed, computation varies based on metric used (edge case avg?)
//        fun computeAggregationsWithoutOverlap(intervalAggregations: InternalAggregations, start: Long, end: Long): InternalAggregations {
//            /*
//            PSUEDOCODE
//            1. Look at first bucket to see which aggs where in initial request
//            2. Store in a map of {aggregationName: [aggType, runningSum/Min/Max/Avg to change]
//            3. Iterate through all buckets
//                4. if bucket in range start <= timeStamp < end
//                    5. update all computation values in map
//            6. init new InternalAggregations object
//            7. iterate throguh all key, vals in map and construct an internalAggregation object for each of them, add to InternalAggregations object
//            8. return InternalAggregations object
//             */
//            intervalAggregations.get<>()
//            val result = listOf(intervalAggregations)
//            return InternalAggregations(intervalAggregations, null)
//        }
        @Suppress("UNCHECKED_CAST")
        override fun handleResponse(response: T?) {
            // Handle the response if it came from intercpetor
            when (response) {
                // live index
                is QuerySearchResult -> {
                    if (response.hasAggs() && isRewrittenInterceptorRequest(response)) {
                        // Check for overlap
                        val (startTime, endTime) = findOverlap(response)
                        logger.error("ronsax: live index: start $startTime and end $endTime")
                        // Modify agg to be original result without overlap computed in
                        // TODO handle overlap here
                        // TODO create a copy of the QuerySearchResult with aggregations modified
//                        val newQuerySerach = QuerySearchResult()
//                        val responseForHandler = newQuerySerach as T
//                        response.shardIndex = response.shardSearchRequest?.shardId()?.id ?: -1
                        originalHandler?.handleResponse(response)
                    } else {
                        originalHandler?.handleResponse(response)
                    }
                }
                // when just 1 rollup index is in request, keep for testing
                is QueryFetchSearchResult -> {
                    val queryResult = response.queryResult()
                    if (queryResult.hasAggs() && isRewrittenInterceptorRequest(queryResult)) {
                        // Check for overlap
                        val (startTime, endTime) = findOverlap(queryResult)
                        logger.error("ronsax: rollup index: start $startTime and end $endTime")
//                        response.shardIndex = response.shardSearchRequest?.shardId()?.id ?: -1

                        // TODO handle overlap here
                        // TODO change response object
//                        val r1 = QueryFetchSearchResult(response.queryResult(), response.fetchResult())
//                        r1.shardIndex = response.shardIndex
//                        val r2: T = r1 as T
                        originalHandler?.handleResponse(response)
                    } else {
                        originalHandler?.handleResponse(response)
                    }
                } else -> {
                    // Delegate to original handler
                    originalHandler?.handleResponse(response)
                }
            }
        }

        override fun handleException(exp: TransportException?) {
            // Handle exceptions or delegate to the original handler
            originalHandler?.handleException(exp)
        }

        override fun executor(): String {
            return originalHandler?.executor() ?: ""
        }
    }
}
