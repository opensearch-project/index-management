/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import kotlinx.coroutines.*
import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.BoostingQueryBuilder
import org.opensearch.index.query.ConstantScoreQueryBuilder
import org.opensearch.index.query.DisMaxQueryBuilder
import org.opensearch.index.query.MatchAllQueryBuilder
import org.opensearch.index.query.MatchPhraseQueryBuilder
import org.opensearch.index.query.QueryBuilder
import org.opensearch.index.query.QueryStringQueryBuilder
import org.opensearch.index.query.RangeQueryBuilder
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.index.query.TermsQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.search.MatchQuery
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
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportChannel
import org.opensearch.transport.TransportInterceptor
import org.opensearch.transport.TransportRequest
import org.opensearch.transport.TransportRequestHandler
import java.util.Date
import java.util.concurrent.CountDownLatch

private val logger = LogManager.getLogger(RollupInterceptor::class.java)
//private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
class RollupInterceptor(
    val clusterService: ClusterService,
    val settings: Settings,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    val client: Client
) : TransportInterceptor,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("RollupInterceptor")){

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
    /*
    Returns tuple (isOverlap, min(Live Data Date)) to filter out overlapping data in rollup query
     */
    fun isOverlap(rollupIndex: String, liveIndex: String, job: Rollup): Pair<Boolean, Date> {
        /*
        1. Get the date_histogram source_field & target field from the rollup job
        2. Query rollup index to find the maxRolledDate
        3. Query live index to find the minLiveDate
        4. Check for intersection and return values
         */
        var dateSourceField: String = ""
        var dateTargetField: String = ""
        for (dim in job.dimensions) {
            if (dim.type == Dimension.Type.DATE_HISTOGRAM) {
                dateSourceField = dim.sourceField
                dateTargetField = dim.targetField
                break
            }
        }
        /*
        Query to find minLiveDate
        {
            "size": 0,
            "aggs": {
                "minDate": {
                    "min": {
                        "date_histogram": "tpep_pickup_datetime"
                    }
                }
            }
        }
         */
//        val queryJson ="{\"size\":0,\"aggs\":{\"minDate\":{\"min\":{\"field\":$dateSourceField}}}}" //TODO check if this query is formatted correctly
//        val searchSourceBuilder = SearchSourceBuilder()
//            .query(QueryBuilders.wrapperQuery(queryJson))
//
//        val minLiveDateRequest = SearchRequest()
//            .source(searchSourceBuilder)
//            .indices(liveIndex)
//
//        val minLiveDateResponse: SearchResponse = client.search(minLiveDateRequest).get()
////        logger.error("ronsax: aggs are ${minLiveDateResponse.aggregations}")
//        var minLiveDate = Date()
//        var maxRolledDate = Date()
        logger.error("ronsax in helper $rollupIndex $dateTargetField $liveIndex $dateSourceField")
        return Pair(false, Date())
    }

    @Suppress("SpreadOperator")
    override fun <T : TransportRequest> interceptHandler(
        action: String,
        executor: String,
        forceExecution: Boolean,
        actualHandler: TransportRequestHandler<T>
    ): TransportRequestHandler<T> {
        return TransportRequestHandler<T> { request, channel, task ->
            if (searchEnabled && request is ShardSearchRequest && isRollupIndex(request.shardId().indexName, clusterService.state())) {
                val index = request.shardId().indexName

                //                        if (request.source().size() != 0) {
                //                            throw IllegalArgumentException("Rollup search must have size explicitly set to 0, but found ${request.source().size()}")
                //                        }
                val indices = request.indices().map { it.toString() }.toTypedArray()
                val concreteIndices = indexNameExpressionResolver
                    .concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
                val concreteRolledIndexNames = mutableListOf<String>()
                var liveIndexTest = ""
                var rollupIndexTest = ""
                for (indexName in concreteIndices) {
                    if (isRollupIndex(indexName, clusterService.state())) {
                        concreteRolledIndexNames.add(indexName)
                        liveIndexTest = indexName
                    } else {
                        rollupIndexTest = indexName
                    }
                }
                val filteredConcreteIndices = concreteRolledIndexNames.toTypedArray()
                val rollupJob = clusterService.state().metadata.index(index).getRollupJobs()?.get(0)
                    ?: throw IllegalArgumentException("No rollup job associated with target_index")
                try {
                    val latch = CountDownLatch(1)
                    val thread = Thread {
                        val (isOverlap, minLiveDataDate) = isOverlap(rollupIndexTest, liveIndexTest, rollupJob)
                        logger.error("ronsax in intercept $isOverlap $minLiveDataDate")
                        latch.countDown()
                    }
                    thread.start()
                    latch.await()
                    logger.error("ronsax the thread finished baby!!")
                } catch (e: Exception) {
                    logger.error(e)
                }
                // To extract fields from QueryStringQueryBuilder we need concrete source index name.
                // TODO add prefix to query if there is overlap
                val queryFieldMappings = getQueryMetadata(
                    request.source().query(),
                    getConcreteSourceIndex(rollupJob.sourceIndex, indexNameExpressionResolver, clusterService.state())
                )
                val aggregationFieldMappings = getAggregationMetadata(request.source().aggregations()?.aggregatorFactories)
                val fieldMappings = queryFieldMappings + aggregationFieldMappings

                val allMatchingRollupJobs = validateIndicies(filteredConcreteIndices, fieldMappings)

                // only rebuild if there is necessity to rebuild
                if (fieldMappings.isNotEmpty()) {
                    rewriteShardSearchForRollupJobs(request, allMatchingRollupJobs)
                }
            }
            actualHandler.messageReceived(request, channel, task)
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
