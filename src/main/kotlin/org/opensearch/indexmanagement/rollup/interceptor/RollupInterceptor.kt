/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchRequestBuilder
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.SearchTask
import org.opensearch.action.support.IndicesOptions
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.settings.Settings
import org.opensearch.index.mapper.ObjectMapper
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.BoostingQueryBuilder
import org.opensearch.index.query.ConstantScoreQueryBuilder
import org.opensearch.index.query.DisMaxQueryBuilder
import org.opensearch.index.query.MatchAllQueryBuilder
import org.opensearch.index.query.MatchPhraseQueryBuilder
import org.opensearch.index.query.QueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.query.QueryStringQueryBuilder
import org.opensearch.index.query.RangeQueryBuilder
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.index.query.TermsQueryBuilder
import org.opensearch.index.search.MatchQuery
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupFieldMapping
import org.opensearch.indexmanagement.rollup.model.RollupFieldMapping.Companion.UNKNOWN_MAPPING
import org.opensearch.indexmanagement.rollup.query.QueryStringQueryUtil
import org.opensearch.indexmanagement.rollup.settings.RollupSettings
import org.opensearch.indexmanagement.rollup.util.getDateHistogram
import org.opensearch.indexmanagement.rollup.util.getRollupJobs
import org.opensearch.indexmanagement.rollup.util.isRollupIndex
import org.opensearch.indexmanagement.rollup.util.populateFieldMappings
import org.opensearch.indexmanagement.rollup.util.rewriteSearchSourceBuilder
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.search.aggregations.AggregationBuilder
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.internal.ShardSearchRequest
import org.opensearch.search.sort.SortBuilder
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportChannel
import org.opensearch.transport.TransportInterceptor
import org.opensearch.transport.TransportRequest
import org.opensearch.transport.TransportRequestHandler
import java.util.Date
import java.util.concurrent.CountDownLatch
import java.text.SimpleDateFormat

private val logger = LogManager.getLogger(RollupInterceptor::class.java)
class RollupInterceptor(
    val clusterService: ClusterService,
    val settings: Settings,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    val client: Client
) : TransportInterceptor {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var searchEnabled = RollupSettings.ROLLUP_SEARCH_ENABLED.get(settings)
    @Volatile private var searchAllJobs = RollupSettings.ROLLUP_SEARCH_ALL_JOBS.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(RollupSettings.ROLLUP_SEARCH_ENABLED) {
            searchEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(RollupSettings.ROLLUP_SEARCH_ALL_JOBS) {
            searchAllJobs = it
        }
    }
    suspend fun logIndex(index: String) {
        val searchRequest = SearchRequest()
            .indices(index)

        val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }
        logger.error("ronsax response is $response")
    }

    private fun convertEpochMillisToDateString(epochMillis: Long): String {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = SimpleDateFormat(pattern)
        val date = Date(epochMillis)
        val dateString = dateFormat.format(date)
        return dateString
    }
    private fun convertDateStringToEpochMillis(dateString: String): Long {
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
    private fun convertFixedIntervalStringToMs(fixedInterval:String): Long {
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
    /*
    Modifies search request to handle overlap between rollup index and live index
     */
    private fun mergeRollupAndLiveIndex(rollupIndex: String, liveIndex: String, job: Rollup, request: ShardSearchRequest) {
        /*
        1. Get the date_histogram source_field & target field from the rollup job
        2. Query rollup index to find the maxRolledDataDate
        3. Query live index to find the minLiveDataDate
        4. Check for intersection and modify search request
         */
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
        val fakeQuery = QueryBuilders.boolQuery()
            .must(request.source().query())
            .must(QueryBuilders.matchAllQuery())
            .must(QueryBuilders.matchAllQuery())
            .must(QueryBuilders.matchAllQuery())

        // Build search request to find the minimum date in the live data index
        var sort = SortBuilders.fieldSort(dateSourceField).order(SortOrder.ASC)
        var searchSourceBuilder = SearchSourceBuilder()
            .sort(sort)
            .size(1)
            .query(fakeQuery)
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
            })
        latch.await()

        // Build search request to find the maximum date on the rolled data index
        sort = SortBuilders.fieldSort("${dateTargetField}.date_histogram").order(SortOrder.DESC)
        searchSourceBuilder = SearchSourceBuilder()
            .sort(sort)
            .query(fakeQuery)
            .size(1)
//            .query(QueryBuilders.wrapperQuery(queryJson))
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
            })
        latch.await()

        if (minLiveDateResponse != null && maxRolledDateResponse != null) {
            // Rollup data ends at maxRolledDate + fixedInterval
            val maxRolledDate: Long = maxRolledDateResponse!!.hits.hits[0].sourceAsMap.get("${dateTargetField}.date_histogram") as Long
            val rollupDataEndPoint = maxRolledDate + convertFixedIntervalStringToMs(fixedInterval = rollupInterval!!)
            val minLiveDate = minLiveDateResponse!!.hits.hits[0].sourceAsMap.get("$dateSourceField") as String
            val liveDataStartPoint = convertDateStringToEpochMillis(minLiveDate)
            if (liveDataStartPoint <= rollupDataEndPoint) {
                // Find intersection timestamp
                val intersectionTime = maxRolledDate
                val shardRequestIndex = request.shardId().indexName
                // Change request to include overlap filter
                if (shardRequestIndex == liveIndex) {
                    val filterOverlapQuery = QueryBuilders.boolQuery()
                        .must(QueryBuilders.rangeQuery(dateSourceField).gte(convertEpochMillisToDateString(intersectionTime))) // fix date format
                    val combinedQuery: BoolQueryBuilder = QueryBuilders.boolQuery()
                        .must(request.source().query())
                        .must(filterOverlapQuery)
                    request.source().query(combinedQuery)
                } else if (shardRequestIndex == rollupIndex) {
                    val filterOverlapQuery = QueryBuilders.boolQuery()
                        .must(QueryBuilders.rangeQuery(dateSourceField).lt(intersectionTime)) // fix date format
                    val combinedQuery: BoolQueryBuilder = QueryBuilders.boolQuery()
                        .must(request.source().query())
                        .must(filterOverlapQuery)
                    request.source().query(combinedQuery)
                }
            }
        } else {
            logger.error("Unable to do client calls in mergeRollupAndLiveIndex function")
        }

        return
    }
    private fun mergeLiveIndexIntervals(liveIndices: Array<String>, rollupIndices: Array<String>, job: Rollup, request: ShardSearchRequest) {
        /* merge intervals */
        mergeRollupAndLiveIndex(rollupIndex = rollupIndices[0], liveIndex = liveIndices[0], job = job, request = request)
    }
    // Returns Pair<containsRollup: Boolean, rollupJob: RollupJob>
    private fun originalSearchContainsRollup(request: ShardSearchRequest): Pair<Boolean, Rollup?> {
        val indices = request.indices().map { it.toString() }.toTypedArray()
        val allIndices = indexNameExpressionResolver
            .concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
        for (index in allIndices) {
            if (isRollupIndex(index, clusterService.state())) {
                val rollupJob = clusterService.state().metadata.index(index).getRollupJobs()?.get(0)
                return Pair(true, rollupJob)
            }
        }
        return Pair(false, null)
    }
    // Need to determine if this was an internal client call to avoid infinite loop from interceptor, -> query string doesn't include "query"
    fun isInternalSearchRequest(request: ShardSearchRequest): Boolean {
        val jsonRequest: String = request.source().query().toString()
        // Detected dummy field from internal search request
        if (jsonRequest.contains(",\n" +
                    "      {\n" +
                    "        \"match_all\" : {\n" +
                    "          \"boost\" : 1.0\n" +
                    "        }\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"match_all\" : {\n" +
                    "          \"boost\" : 1.0\n" +
                    "        }\n" +
                    "      },\n" +
                    "      {\n" +
                    "        \"match_all\" : {\n" +
                    "          \"boost\" : 1.0\n" +
                    "        }\n" +
                    "      }")) {
            return true
        }

        return false
    }

    @Suppress("SpreadOperator")
    override fun <T : TransportRequest> interceptHandler(
        action: String,
        executor: String,
        forceExecution: Boolean,
        actualHandler: TransportRequestHandler<T>
    ): TransportRequestHandler<T> {
        return object : TransportRequestHandler<T> {
            override fun messageReceived(request: T, channel: TransportChannel, task: Task) {
                if (searchEnabled && request is ShardSearchRequest) {
                    val (containsRollup, rollupJob) = originalSearchContainsRollup(request)
                    // Only modifies rollup searches and avoids internal client calls
                    if (containsRollup) {
                        val indices = request.indices().map { it.toString() }.toTypedArray()
                        val concreteIndices = indexNameExpressionResolver
                            .concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
                        val concreteRolledIndexNames = mutableListOf<String>()
                        val concreteLiveIndexNames = mutableListOf<String>()
                        for (indexName in concreteIndices) {
                            if (isRollupIndex(indexName, clusterService.state())) {
                                concreteRolledIndexNames.add(indexName)
                            } else {
                                concreteLiveIndexNames.add(indexName)
                            }
                        }
                        val concreteRollupIndicesArray = concreteRolledIndexNames.toTypedArray()
                        val concreteLiveIndicesArray = concreteLiveIndexNames.toTypedArray()
                        val shardRequestIndex = request.shardId().indexName
                        // Check before rewriting rollup because it deletes dummy field
                        val requestCalledInInterceptor = isInternalSearchRequest(request)
                        // Rewrite the request to fit rollup format
                        if (isRollupIndex(shardRequestIndex, clusterService.state())) {
//                            if (!requestCalledInInterceptor && request.source().size() != 0) {
//                                throw IllegalArgumentException("Rollup search must have size explicitly set to 0, but found ${request.source().size()}")
//                            }
                            // To extract fields from QueryStringQueryBuilder we need concrete source index name.
                            val queryFieldMappings = getQueryMetadata(
                                request.source().query(),
                                getConcreteSourceIndex(rollupJob!!.sourceIndex, indexNameExpressionResolver, clusterService.state())
                            )
                            val aggregationFieldMappings = getAggregationMetadata(request.source().aggregations()?.aggregatorFactories)
                            val fieldMappings = queryFieldMappings + aggregationFieldMappings

                            val allMatchingRollupJobs = validateIndicies(concreteRollupIndicesArray, fieldMappings)

                            // only rebuild if there is necessity to rebuild
                            if (fieldMappings.isNotEmpty()) {
                                rewriteShardSearchForRollupJobs(request, allMatchingRollupJobs)
                            }
                        }
                        // avoid infinite interceptor loop
                        if (concreteRollupIndicesArray.isNotEmpty() && concreteLiveIndicesArray.isNotEmpty() && !requestCalledInInterceptor) {
                            mergeLiveIndexIntervals(concreteLiveIndicesArray, concreteRollupIndicesArray, rollupJob!!, request)
                        }
                    }
                }
                actualHandler.messageReceived(request, channel, task)
            }
        }
    }

    fun getConcreteSourceIndex(sourceIndex: String, resolver: IndexNameExpressionResolver, clusterState: ClusterState): String {
        val concreteIndexNames = resolver.concreteIndexNames(clusterState, IndicesOptions.LENIENT_EXPAND_OPEN, sourceIndex)
        if (concreteIndexNames.isEmpty()) {
            logger.warn("Cannot resolve rollup sourceIndex [$sourceIndex]")
            return ""
        }

        var concreteIndexName: String = ""
        if (concreteIndexNames.size == 1 && IndexUtils.isConcreteIndex(concreteIndexNames[0], clusterState)) {
            concreteIndexName = concreteIndexNames[0]
        } else if (concreteIndexNames.size > 1) {
            concreteIndexName = IndexUtils.getNewestIndexByCreationDate(concreteIndexNames, clusterState)
        } else if (IndexUtils.isAlias(sourceIndex, clusterState) || IndexUtils.isDataStream(sourceIndex, clusterState)) {
            concreteIndexName = IndexUtils.getWriteIndex(sourceIndex, clusterState)
                ?: IndexUtils.getNewestIndexByCreationDate(concreteIndexNames, clusterState) //
        }
        return concreteIndexName
    }

    /*
    * Validate that all indices have rollup job which matches field mappings from request
    * TODO return compiled list of issues here instead of just throwing exception
    * */
    private fun validateIndicies(concreteIndices: Array<String>, fieldMappings: Set<RollupFieldMapping>): Map<Rollup, Set<RollupFieldMapping>> {
        var allMatchingRollupJobs: Map<Rollup, Set<RollupFieldMapping>> = mapOf()
        for (concreteIndex in concreteIndices) {
            val rollupJobs = clusterService.state().metadata.index(concreteIndex).getRollupJobs()
                ?: throw IllegalArgumentException("Not all indices have rollup job: $concreteIndex")

            val (matchingRollupJobs, issues) = findMatchingRollupJobs(fieldMappings, rollupJobs)
            if (issues.isNotEmpty() || matchingRollupJobs.isEmpty()) {
                throw IllegalArgumentException("Could not find a rollup job that can answer this query because $issues")
            }
            allMatchingRollupJobs += matchingRollupJobs
        }
        return allMatchingRollupJobs
    }

    @Suppress("ComplexMethod")
    private fun getAggregationMetadata(
        aggregationBuilders: Collection<AggregationBuilder>?,
        fieldMappings: MutableSet<RollupFieldMapping> = mutableSetOf()
    ): Set<RollupFieldMapping> {
        aggregationBuilders?.forEach {
            when (it) {
                is TermsAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.field(), it.type))
                }
                is DateHistogramAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.field(), it.type))
                }
                is HistogramAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.field(), it.type))
                }
                is SumAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is AvgAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is MaxAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is MinAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                is ValueCountAggregationBuilder -> {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, it.field(), it.type))
                }
                else -> throw IllegalArgumentException("The ${it.type} aggregation is not currently supported in rollups")
            }
            if (it.subAggregations?.isNotEmpty() == true) {
                getAggregationMetadata(it.subAggregations, fieldMappings)
            }
        }
        return fieldMappings
    }

    @Suppress("ComplexMethod", "ThrowsCount", "LongMethod")
    private fun getQueryMetadata(
        query: QueryBuilder?,
        concreteSourceIndexName: String?,
        fieldMappings: MutableSet<RollupFieldMapping> = mutableSetOf()
    ): Set<RollupFieldMapping> {
        if (query == null) {
            return fieldMappings
        }
        when (query) {
            is TermQueryBuilder -> {
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), Dimension.Type.TERMS.type))
            }
            is TermsQueryBuilder -> {
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), Dimension.Type.TERMS.type))
            }
            is RangeQueryBuilder -> {
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), UNKNOWN_MAPPING))
            }
            is MatchAllQueryBuilder -> {
                // do nothing
            }
            is BoolQueryBuilder -> {
                query.must()?.forEach { this.getQueryMetadata(it, concreteSourceIndexName, fieldMappings) }
                query.mustNot()?.forEach { this.getQueryMetadata(it, concreteSourceIndexName, fieldMappings) }
                query.should()?.forEach { this.getQueryMetadata(it, concreteSourceIndexName, fieldMappings) }
                query.filter()?.forEach { this.getQueryMetadata(it, concreteSourceIndexName, fieldMappings) }
            }
            is BoostingQueryBuilder -> {
                this.getQueryMetadata(query.positiveQuery(), concreteSourceIndexName, fieldMappings)
                this.getQueryMetadata(query.negativeQuery(), concreteSourceIndexName, fieldMappings)
            }
            is ConstantScoreQueryBuilder -> {
                this.getQueryMetadata(query.innerQuery(), concreteSourceIndexName, fieldMappings)
            }
            is DisMaxQueryBuilder -> {
                query.innerQueries().forEach { this.getQueryMetadata(it, concreteSourceIndexName, fieldMappings) }
            }
            is MatchPhraseQueryBuilder -> {
                if (!query.analyzer().isNullOrEmpty() || query.slop() != MatchQuery.DEFAULT_PHRASE_SLOP ||
                    query.zeroTermsQuery() != MatchQuery.DEFAULT_ZERO_TERMS_QUERY
                ) {
                    throw IllegalArgumentException(
                        "The ${query.name} query is currently not supported with analyzer/slop/zero_terms_query in rollups"
                    )
                }
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), Dimension.Type.TERMS.type))
            }
            is QueryStringQueryBuilder -> {
                if (concreteSourceIndexName.isNullOrEmpty()) {
                    throw IllegalArgumentException("Can't parse query_string query without sourceIndex mappings!")
                }
                // Throws IllegalArgumentException if unable to parse query
                val (queryFields, otherFields) = QueryStringQueryUtil.extractFieldsFromQueryString(query, concreteSourceIndexName)
                for (field in queryFields) {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, field, Dimension.Type.TERMS.type))
                }
                for (field in otherFields.keys) {
                    fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, field, Dimension.Type.TERMS.type))
                }
            }
            else -> {
                throw IllegalArgumentException("The ${query.name} query is currently not supported in rollups")
            }
        }
        return fieldMappings
    }

    // TODO: How does this job matching work with roles/security?
    @Suppress("ComplexMethod")
    private fun findMatchingRollupJobs(
        fieldMappings: Set<RollupFieldMapping>,
        rollupJobs: List<Rollup>
    ): Pair<Map<Rollup, Set<RollupFieldMapping>>, Set<String>> {
        val rollupFieldMappings = rollupJobs.map { rollup ->
            rollup to rollup.populateFieldMappings()
        }.toMap()

        val knownFieldMappings = mutableSetOf<RollupFieldMapping>()
        val unknownFields = mutableSetOf<String>()

        fieldMappings.forEach {
            if (it.mappingType == UNKNOWN_MAPPING) unknownFields.add(it.fieldName)
            else knownFieldMappings.add(it)
        }

        val potentialRollupFieldMappings = rollupFieldMappings.filterValues {
            it.containsAll(knownFieldMappings) && it.map { rollupFieldMapping -> rollupFieldMapping.fieldName }.containsAll(unknownFields)
        }

        val issues = mutableSetOf<String>()
        if (potentialRollupFieldMappings.isEmpty()) {
            // create a global set of all field mappings
            val allFieldMappings = mutableSetOf<RollupFieldMapping>()
            rollupFieldMappings.values.forEach { allFieldMappings.addAll(it) }

            // create a global set of field names to handle unknown mapping types
            val allFields = allFieldMappings.map { it.fieldName }

            // Adding to the issue if cannot find defined field mapping or if the field is missing
            fieldMappings.forEach {
                if (!allFields.contains(it.fieldName)) issues.add(it.toIssue(true))
                else if (it.mappingType != UNKNOWN_MAPPING && !allFieldMappings.contains(it)) issues.add(it.toIssue())
            }
        }

        return potentialRollupFieldMappings to issues
    }

    // TODO: revisit - not entirely sure if this is the best thing to do, especially when there is a range query
    private fun pickRollupJob(rollups: Set<Rollup>): Rollup {
        if (rollups.size == 1) {
            return rollups.first()
        }
        // Make selection deterministic
        val sortedRollups = rollups.sortedBy { it.id }

        // Picking the job with largest rollup window for now
        return sortedRollups.reduce { matched, new ->
            if (getEstimateRollupInterval(matched) > getEstimateRollupInterval(new)) matched
            else new
        }
    }

    private fun getEstimateRollupInterval(rollup: Rollup): Long {
        return if (rollup.getDateHistogram().calendarInterval != null) {
            DateHistogramInterval(rollup.getDateHistogram().calendarInterval).estimateMillis()
        } else {
            DateHistogramInterval(rollup.getDateHistogram().fixedInterval).estimateMillis()
        }
    }

    private fun rewriteShardSearchForRollupJobs(request: ShardSearchRequest, matchingRollupJobs: Map<Rollup, Set<RollupFieldMapping>>) {
        val matchedRollup = pickRollupJob(matchingRollupJobs.keys)
        val fieldNameMappingTypeMap = matchingRollupJobs.getValue(matchedRollup).associateBy({ it.fieldName }, { it.mappingType })
        val concreteSourceIndex = getConcreteSourceIndex(matchedRollup.sourceIndex, indexNameExpressionResolver, clusterService.state())
        if (searchAllJobs) {
            request.source(request.source().rewriteSearchSourceBuilder(matchingRollupJobs.keys, fieldNameMappingTypeMap, concreteSourceIndex))
        } else {
            request.source(request.source().rewriteSearchSourceBuilder(matchedRollup, fieldNameMappingTypeMap, concreteSourceIndex))
        }
    }
}
