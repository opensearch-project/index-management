/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.search.DocValueFormat
import org.opensearch.search.aggregations.InternalAggregation
import org.opensearch.search.aggregations.InternalAggregations
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.internal.ShardSearchRequest
import org.opensearch.search.query.QuerySearchResult
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.util.convertDateStringToEpochMillis
import org.opensearch.indexmanagement.rollup.util.convertFixedIntervalStringToMs
import org.opensearch.indexmanagement.rollup.util.getRollupJobs
import org.opensearch.indexmanagement.rollup.util.isRollupIndex
import org.opensearch.indexmanagement.rollup.util.zonedDateTimeToMillis
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
import kotlin.math.max
import kotlin.math.min

/**
* The Response Interceptor class modifies resopnses if the search API is triggered on rollup and live data
* 1. The class checks if the request was rewritten in the RollupInterceptor into more granular buckets
* 2. in findOverlap it checks for overlap between live data and rollup data and returns the interval to include
* 3. computeAggregationsWithoutOverlap() iterates through the buckets and recomputes the aggregations in the expected format
* 4. Returns a new response for each shard to be combined later
 **/
class ResponseInterceptor(
    val clusterService: ClusterService,
    val settings: Settings,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    val client: Client
) : TransportInterceptor,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("Rollup Response Interceptor")) {
    private val logger = LogManager.getLogger(javaClass)
    override fun interceptSender(sender: TransportInterceptor.AsyncSender): TransportInterceptor.AsyncSender {
        return CustomAsyncSender(sender)
    }

    private inner class CustomAsyncSender(private val originalSender: TransportInterceptor.AsyncSender) : TransportInterceptor.AsyncSender {
        override fun <T : TransportResponse> sendRequest(
            connection: Transport.Connection,
            action: String,
            request: TransportRequest,
            options: TransportRequestOptions,
            handler: TransportResponseHandler<T>
        ) {
            val interceptedHandler = CustomResponseHandler(handler)

            originalSender.sendRequest(connection, action, request, options, interceptedHandler)
        }
    }

    @Suppress("TooManyFunctions")
    private inner class CustomResponseHandler<T : TransportResponse?>(
        private val originalHandler: TransportResponseHandler<T>?
    ) : TransportResponseHandler<T> {
        override fun read(inStream: StreamInput?): T {
            val response = originalHandler?.read(inStream)
            return response!!
        }

        /**
         * Check if this response was modified in the request interceptor
         * and should be put back together
         * @param QuerySearchResult
         * @return Boolean
         */
        fun isRewrittenInterceptorRequest(response: QuerySearchResult): Boolean {
            val currentAggregations = response.aggregations().expand()
            for (agg in currentAggregations) {
                if (agg.name == "interceptor_interval_data") {
                    return true
                }
            }
            return false
        }
        @Suppress("SpreadOperator")
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
        @Suppress("SpreadOperator")
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

        /**
         * Calculates the end time for the current shard index if it is a rollup index with data overlapp
         * @params liveDataStartPoint: Long, rollupIndices: Array<String>, dateTargetField: String
         * @return Long
         **/
        @Suppress("SpreadOperator")
        suspend fun getRollupEndTime(liveDataStartPoint: Long, rollupIndices: Array<String>, dateTargetField: String): Long {
            // Build search request to find the maximum rollup timestamp <= liveDataStartPoint
            val sort = SortBuilders.fieldSort("$dateTargetField.date_histogram").order(SortOrder.DESC)
            val query = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery(dateTargetField).lte(liveDataStartPoint))
            val searchSourceBuilder = SearchSourceBuilder()
                .sort(sort)
                .query(query)
                .size(1)
            // Need to avoid infinite interceptor loop
            val req = SearchRequest()
                .source(searchSourceBuilder)
                .indices(*rollupIndices)
            val res = client.suspendUntil { search(req, it) }
            try {
                return res!!.hits.hits[0].sourceAsMap.get("$dateTargetField.date_histogram") as Long
            } catch (e: Exception) {
                logger.error("Not able to retrieve intersection time from response: ", e)
            }
            return 0L // dummy :P
        }
        /**
         * Checks for overlap in timeseries data and returns the non overlapping interval to include
         * Note startRange is inclusive and endRange is exclusive, they are Longs because the type is epoch milliseconds
         * @param QuerySearchResult
         * @return Pair(startRange: Long, endRange: Long)
         **/
        // TODO intercept at the index level instead of the shard level to avoid redundant client calls for every index
        @Suppress("LongMethod", "SpreadOperator")
        suspend fun findOverlap(response: QuerySearchResult): Pair<Long, Long> {
            // TODO add more error logs and try catch statements for client calls
            val job: Rollup = getRollupJob(response)!!
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
            val request: ShardSearchRequest = response.shardSearchRequest!!
            val oldQuery = request.source().query()
            val (rollupIndices, liveIndices) = getRollupAndLiveIndices(request)
            val shardRequestIndex = request.shardId().indexName
            val isShardIndexRollup = isRollupIndex(shardRequestIndex, clusterService.state())
            // Build search request to find the maximum date in all rollup indices
            var sort = SortBuilders.fieldSort("$dateTargetField.date_histogram").order(SortOrder.DESC)
            var searchSourceBuilder = SearchSourceBuilder()
                .sort(sort)
                .query(oldQuery)
                .size(1)
            // Need to avoid infinite interceptor loop
            val maxRollupDateRequest = SearchRequest()
                .source(searchSourceBuilder)
                .indices(*rollupIndices) // add all rollup indices to this request
            logger.debug("Sending maxRollupDate request for $shardRequestIndex")
            val maxRollupDateResponse: SearchResponse? = client.suspendUntil { search(maxRollupDateRequest, it) }
            // Build search request to find the minimum date in all live indices
            sort = SortBuilders.fieldSort(dateSourceField).order(SortOrder.ASC)
            searchSourceBuilder = SearchSourceBuilder()
                .sort(sort)
                .size(1)
            val minLiveDateRequest = SearchRequest()
                .source(searchSourceBuilder)
            /*
            If the response shard index is a rollup index, need to find the minimum value of all the live indices to compute the overlap
            This is because I am comparing this index to all the live data to compute the interval I want to keep
            If the response shard index is a live index, need to only compute minimum value of the current shard index
             */
            if (isShardIndexRollup) {
                minLiveDateRequest.indices(*liveIndices)
            } else { // shard index is live index
                minLiveDateRequest.indices(shardRequestIndex)
            }
            logger.debug("Sending minLiveData request for $shardRequestIndex")
            var minLiveDateResponse: SearchResponse? = client.suspendUntil { search(minLiveDateRequest, it) }
            val foundMinAndMax = (minLiveDateResponse != null && maxRollupDateResponse != null)
            // if they overlap find part to exclude
            if (foundMinAndMax && minLiveDateResponse!!.hits.hits.isNotEmpty() && maxRollupDateResponse!!.hits.hits.isNotEmpty()) {
                // Rollup data ends at maxRolledDate + fixedInterval
                val maxRolledDate: Long = maxRollupDateResponse.hits.hits[0].sourceAsMap.get("$dateTargetField.date_histogram") as Long
                val rollupDataEndPoint = maxRolledDate + convertFixedIntervalStringToMs(fixedInterval = rollupInterval!!)
                val minLiveDate = minLiveDateResponse.hits.hits[0].sourceAsMap.get("$dateSourceField") as String
                val liveDataStartPoint = convertDateStringToEpochMillis(minLiveDate)
                // If intersection found on rollup index, remove overlap
                if ((liveDataStartPoint < rollupDataEndPoint) && isShardIndexRollup) {
                    // Start at 0, end at live data
                    logger.debug("Sending request to find rollup endtime for index: $shardRequestIndex")
                    val endTime = getRollupEndTime(liveDataStartPoint, rollupIndices, dateTargetField)
                    return Pair(0L, endTime)
                }
            }
            // No overlap or is live data index so start and end include everything
            return Pair(0L, Long.MAX_VALUE)
        }

        /**
         * Depending on which metric the aggregation is, computation is different
         * @params agg: org.opensearch.search.aggregations.Aggregation, currentValue: Any
         * @return Pair<Any, String>
         */
        @Suppress("ReturnCount")
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
        @Suppress("ReturnCount")
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
        @Suppress("ReturnCount")
        fun createNewMetricAgg(aggName: String, aggValue: Any, aggType: String): InternalAggregation {
            when (aggType) {
                "sum" -> return InternalSum(aggName, (aggValue as Double), DocValueFormat.RAW, null)
                "min" -> return InternalMin(aggName, (aggValue as Double), DocValueFormat.RAW, null)
                "max" -> return InternalMax(aggName, (aggValue as Double), DocValueFormat.RAW, null)
                "value_count" -> return InternalValueCount(aggName, (aggValue as Long), null)
                else -> throw IllegalArgumentException("Could not recreate an aggregation for type $aggType")
            }
        }
        /**
         * Create original avg aggregation
         * @return InternalAvg
         */

        fun initRollupAvgAgg(
            modifiedName: String,
            value: Any,
            aggValues: MutableMap<String, Pair<Any, String>>,
            addedAggregations: MutableSet<String>
        ): InternalAvg {
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
                // Put in set to avoid adding the aggregation twice
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
            throw NullPointerException("Can't calculate avg agg for rollup index")
        }

        /**
         * Returns a new InternalAggregations that contains merged aggregation(s) with the overlapping data removed
         * @params intervalAggregations: InternalAggregations, start: Long, end: Long
         * @return InternalAggregations
         */
        @Suppress("NestedBlockDepth")
        fun computeAggregationsWithoutOverlap(intervalAggregations: InternalAggregations, start: Long, end: Long): InternalAggregations {
            // Store the running values of the aggregations being computed
            // {aggName: String: Pair<value: Any, type:String>}
            val aggValues = mutableMapOf<String, Pair<Any, String>>()

            // Iterate through each aggregation and bucket
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

            // Create a new InternalAggregations with recomputed values discarding the overlap
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
                        launch {
                            // Check for overlap
                            val (startTime, endTime) = findOverlap(response)
                            // Modify agg to be original result without overlap computed in
                            response.aggregations(computeAggregationsWithoutOverlap(response.aggregations().expand(), startTime, endTime))
                            originalHandler?.handleResponse(response)
                        }
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
