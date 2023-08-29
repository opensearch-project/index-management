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
import org.opensearch.search.DocValueFormat
import org.opensearch.search.aggregations.InternalAggregation
import org.opensearch.search.aggregations.InternalAggregations
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.QueryFetchSearchResult
import org.opensearch.search.internal.ShardSearchRequest
import org.opensearch.search.query.QuerySearchResult
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.aggregations.metrics.InternalAvg
import org.opensearch.search.aggregations.metrics.InternalMax
import org.opensearch.search.aggregations.metrics.InternalMin
import org.opensearch.search.aggregations.metrics.InternalScriptedMetric
import org.opensearch.search.aggregations.metrics.InternalSum
import org.opensearch.search.aggregations.metrics.InternalValueCount
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.transport.Transport
import org.opensearch.transport.TransportException
import org.opensearch.transport.TransportInterceptor
import org.opensearch.transport.TransportRequest
import org.opensearch.transport.TransportRequestOptions
import org.opensearch.transport.TransportResponse
import org.opensearch.transport.TransportResponseHandler
import java.time.ZonedDateTime
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.CountDownLatch
import kotlin.math.max
import kotlin.math.min

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
        fun convertDateStringToEpochMillis(dateString: String): Long {
            val pattern = "yyyy-MM-dd HH:mm:ss"
            val formatter = DateTimeFormatter.ofPattern(pattern)
            val localDateTime = LocalDateTime.parse(dateString, formatter)
            val epochMillis = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli()
            return epochMillis
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
        fun getIntersectionTime(liveDataStartPoint: Long, rollupIndices: Array<String>, dateTargetField: String): Long {
            // Build search request to find the minimum rollup timestamp >= liveDataStartPoint
            val sort = SortBuilders.fieldSort("$dateTargetField.date_histogram").order(SortOrder.ASC)
            val query = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery(dateTargetField).gte(liveDataStartPoint))
            val searchSourceBuilder = SearchSourceBuilder()
                .sort(sort)
                .query(query)
                .size(1)
            // Need to avoid infinite interceptor loop
            val req = SearchRequest()
                .source(searchSourceBuilder)
            rollupIndices.forEach { req.indices(it) }
            var res: SearchResponse? = null
            val latch = CountDownLatch(1)
            client.search(
                req,
                object : ActionListener<SearchResponse> {
                    override fun onResponse(searchResponse: SearchResponse) {
                        res = searchResponse
                        latch.countDown()
                    }

                    override fun onFailure(e: Exception) {
                        logger.error("request to find intersection time failed :(", e)
                        latch.countDown()
                    }
                }
            )
            latch.await()
            try {
                return res!!.hits.hits[0].sourceAsMap.get("$dateTargetField.date_histogram") as Long
            } catch (e: Exception) {
                logger.error("Not able to retrieve intersection time from response: $e")
            }
            return 0L // dummy :P
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
            // TODO scale this for multiple indices!!!!
            val (rollupIndices, liveIndices) = getRollupAndLiveIndices(request)
            val shardRequestIndex = request.shardId().indexName
            val isShardIndexRollup = isRollupIndex(shardRequestIndex, clusterService.state())
            // Build search request to find the maximum date on the rolled data index
            var sort = SortBuilders.fieldSort("$dateTargetField.date_histogram").order(SortOrder.DESC)
            var searchSourceBuilder = SearchSourceBuilder()
                .sort(sort)
                .query(oldQuery)
                .size(1)
            // Need to avoid infinite interceptor loop
            val maxRolledDateRequest = SearchRequest()
                .source(searchSourceBuilder)
            rollupIndices.forEach { maxRolledDateRequest.indices(it) } // add all rollup indices to this request
            var maxRolledDateResponse: SearchResponse? = null
            var latch = CountDownLatch(1)
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
            // Build search request to find the minimum date in the live data index
            sort = SortBuilders.fieldSort(dateSourceField).order(SortOrder.ASC)
            searchSourceBuilder = SearchSourceBuilder()
                .sort(sort)
                .size(1)
            val minLiveDateRequest = SearchRequest()
                .source(searchSourceBuilder)
            /*
            If the shard index is a rollup index I want to find the minimum value of all the live indices to compute the overlap
            If the shard index is a live index I only care about the minimum value of the current shard index
             */
            if (isShardIndexRollup) {
                liveIndices.forEach { minLiveDateRequest.indices(it) }
            } else { // shard index is live index
                minLiveDateRequest.indices(shardRequestIndex)
            }

            var minLiveDateResponse: SearchResponse? = null
            latch = CountDownLatch(1)
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
            // if they overlap find part to exclude
            if (minLiveDateResponse != null && maxRolledDateResponse != null && minLiveDateResponse!!.hits.hits.isNotEmpty() && maxRolledDateResponse!!.hits.hits.isNotEmpty()) {
                // Rollup data ends at maxRolledDate + fixedInterval
                val maxRolledDate: Long = maxRolledDateResponse!!.hits.hits[0].sourceAsMap.get("$dateTargetField.date_histogram") as Long
                val rollupDataEndPoint = maxRolledDate + convertFixedIntervalStringToMs(fixedInterval = rollupInterval!!)
                val minLiveDate = minLiveDateResponse!!.hits.hits[0].sourceAsMap.get("$dateSourceField") as String
                val liveDataStartPoint = convertDateStringToEpochMillis(minLiveDate)
                if (liveDataStartPoint < rollupDataEndPoint) {
                    // Find intersection timestamp
                    val intersectionTime = getIntersectionTime(liveDataStartPoint, rollupIndices, dateTargetField)
                    if (isShardIndexRollup) {
                        // Start at 0, end at intersection time
                        return Pair(0L, intersectionTime)
                    } else { // Shard index is live
                        // Start at intersection timestamp, end at inf
                        return Pair(intersectionTime, Long.MAX_VALUE)
                    }
                }
            } else {
                logger.error("Not able to make client calls in response interceptor")
            }
            // No overlap so start and end include everything
            return Pair(0L, Long.MAX_VALUE)
        }
        fun zonedDateTimeToMillis(zonedDateTime: ZonedDateTime): Long {
            return zonedDateTime.toInstant().toEpochMilli()
        }
        // Depending on which metric the aggregation is computer data differently
        fun computeRunningValue(agg: org.opensearch.search.aggregations.Aggregation, currentValue: Any): Pair<Any, String> {
            when (agg) {
                is InternalSum -> {
                    return Pair(agg.value + (currentValue as Double), agg.type)
                }
                is InternalMax -> {
                    return Pair(max(agg.value, (currentValue as Double)), agg.type)
                }
                is InternalMin -> {
                    return Pair(min(agg.value, (currentValue as Double)), agg.type)
                }
                is InternalValueCount -> { // Live data uses this
                    return Pair(agg.value + (currentValue as Long), agg.type)
                }
                is InternalScriptedMetric -> {
                    // Rollup InternalValueCount
                    return Pair((agg.aggregation() as Long) + (currentValue as Long), "value_count")
                }
                else -> throw IllegalArgumentException("This aggregation is not currently supported in rollups searches: ${agg.name}")
            }
        }
        // Depending on which metric the aggregation is return a different start value
        fun getAggComputationStartValue(agg: org.opensearch.search.aggregations.Aggregation): Pair<Any, String> {
            when (agg) {
                is InternalSum -> return Pair(agg.value, agg.type)
                is InternalMax -> return Pair(agg.value, agg.type)
                is InternalMin -> return Pair(agg.value, agg.type)
                is InternalValueCount -> return Pair(agg.value, agg.type) // Live data
                is InternalScriptedMetric -> return Pair(agg.aggregation(), "value_count") // Rollup data
                else -> throw IllegalArgumentException("This aggregation is not currently supported in rollups searches: ${agg.name}")
            }
        }
        fun createNewMetricAgg(aggName: String, aggValue: Any, aggType: String): InternalAggregation {
            when (aggType) {
                "sum" -> return InternalSum(aggName, (aggValue as Double), DocValueFormat.RAW, null)
                "min" -> return InternalMin(aggName, (aggValue as Double), DocValueFormat.RAW, null)
                "max" -> return InternalMax(aggName, (aggValue as Double), DocValueFormat.RAW, null)
                "value_count" -> return InternalValueCount(aggName, (aggValue as Long), null)
//                "scripted_metric" -> {
//                    val script = Script(
//                        ScriptType.INLINE,
//                        "painless",
//                        "long valueCount = 0; for (vc in states) { valueCount += vc } return valueCount;",
//                        emptyMap(), // options
//                        emptyMap()  // params
//                    )
//                    return ScriptedMetricAggregationBuilder(aggName).
//                }
//                "avg" -> return InternalAvg(agg) // TODO look at how to make this bad boy
                else -> throw IllegalArgumentException("Could not recreate an aggregation for type $aggType")
            }
        }
        // Create original avg aggregation
        fun initRollupAvgAgg(modifiedName: String, value: Any, aggValues: MutableMap<String, Pair<Any, String>>, addedAggregations: MutableSet<String>): InternalAvg {
            // Sum calc
            if (modifiedName.contains(".rollup.avg.sum")) {
                // Won't double count
                addedAggregations += modifiedName
                val originalName = modifiedName.removeSuffix(".rollup.avg.sum")
                val avgSum: Double = value as Double
                for ((aggName, data) in aggValues) {
                    // Found value count component to create InternalAvg object
                    if (!addedAggregations.contains(aggName) && aggName.contains(originalName)) {
                        addedAggregations += aggName
                        val (avgCount, _) = data
                        return InternalAvg(originalName, avgSum, (avgCount as Long), DocValueFormat.RAW, null)
                    }
                }
            } else { // Value count calc
                // Won't double count
                addedAggregations += modifiedName
                val originalName = modifiedName.removeSuffix(".rollup.avg.value_count")
                val avgCount = value as Long
                for ((aggName, data) in aggValues) {
                    // Found sum component to create InternalAvg object
                    if (!addedAggregations.contains(aggName) && aggName.contains(originalName)) {
                        addedAggregations += aggName
                        val (avgSum, _) = data
                        return InternalAvg(originalName, (avgSum as Double), avgCount, DocValueFormat.RAW, null)
                    }
                }
            }
            throw Exception("Can't calculate avg agg for rollup index")
        }

//         Returns a new InternalAggregations that contains a merged aggregation(s) with the overlapping data removed, computation varies based on metric used (edge case avg?)
        /**
         * @param intervalAggregations
         * @return Int
         */
        fun computeAggregationsWithoutOverlap(intervalAggregations: InternalAggregations, start: Long, end: Long): InternalAggregations {
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
            // Create an empty map to hold the agg values
            // {aggName: String: Pair<value: Any, type:String>}
            val aggValues = mutableMapOf<String, Pair<Any, String>>()
//
//            // Iterate through each aggregation and bucket
            val interceptorAgg = intervalAggregations.asMap().get("interceptor_interval_data") as InternalDateHistogram
            for (bucket in interceptorAgg.buckets) {
                val zdt = bucket.key as ZonedDateTime
                val timestamp: Long = zonedDateTimeToMillis(zdt)
                // Only consider buckets within the specified range
                // Start is inclusive and end is exclusive
                if (timestamp >= start && timestamp < end) {
                    for (originalAgg in bucket.aggregations) {
                        val aggName = originalAgg.name
                        if (aggValues.containsKey(aggName)) {
                            // Compute running calculation
                            val (currentValue, _) = aggValues[aggName]!!
                            aggValues[aggName] = computeRunningValue(originalAgg!!, currentValue)
                        } else {
                            aggValues[aggName] = getAggComputationStartValue(originalAgg)
                        }
                    }
                }
            }

            // Create a new InternalAggregations with sum values
            val allAggregations = mutableListOf<InternalAggregation>()
            val addedAggregations = mutableSetOf<String>() // avoid repeating the same aggregations
            for ((aggName, data) in aggValues) {
                if (addedAggregations.contains(aggName)) continue
                // special case to compute value_count for rollup indices
                else if (aggName.contains(".rollup.value_count")) {
                    val (value, _) = data
                    val originalName = aggName.removeSuffix(".rollup.value_count")
                    allAggregations.add(InternalValueCount(originalName, value as Long, null))
                    addedAggregations += aggName
                }
                // special case to compute avg agg using sum and value_count calculation
                else if (aggName.contains(".rollup.avg.sum") || aggName.contains(".rollup.avg.value_count")) {
                    val (value, _) = data
                    allAggregations.add(initRollupAvgAgg(aggName, value, aggValues, addedAggregations))
                } else { // Sum, Min, or Max agg
                    val (value, type) = data
                    val newAgg = createNewMetricAgg(aggName, value, type)
                    allAggregations.add(newAgg)
                    addedAggregations += aggName
                }
            }
            return InternalAggregations(allAggregations, null)
        }
        @Suppress("UNCHECKED_CAST")
        override fun handleResponse(response: T?) {
            // Handle the response if it came from interceptor
            when (response) {
                // live index
                is QuerySearchResult -> {
                    if (response.hasAggs() && isRewrittenInterceptorRequest(response)) {
                        // Check for overlap
                        val (startTime, endTime) = findOverlap(response)
                        // Modify agg to be original result without overlap computed in
                        response.aggregations(computeAggregationsWithoutOverlap(response.aggregations().expand(), startTime, endTime))
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
                        // Modify agg to be original result without overlap computed in
                        queryResult.aggregations(computeAggregationsWithoutOverlap(queryResult.aggregations().expand(), startTime, endTime))
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
