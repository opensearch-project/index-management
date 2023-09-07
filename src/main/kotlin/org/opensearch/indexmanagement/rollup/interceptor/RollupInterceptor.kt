/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.IndicesOptions
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.index.query.QueryBuilder
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.index.query.TermsQueryBuilder
import org.opensearch.index.query.RangeQueryBuilder
import org.opensearch.index.query.MatchAllQueryBuilder
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.BoostingQueryBuilder
import org.opensearch.index.query.ConstantScoreQueryBuilder
import org.opensearch.index.query.DisMaxQueryBuilder
import org.opensearch.index.query.MatchPhraseQueryBuilder
import org.opensearch.index.query.QueryStringQueryBuilder
import org.opensearch.index.search.MatchQuery
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupFieldMapping
import org.opensearch.indexmanagement.rollup.model.RollupFieldMapping.Companion.UNKNOWN_MAPPING
import org.opensearch.indexmanagement.rollup.query.QueryStringQueryUtil
import org.opensearch.indexmanagement.rollup.settings.RollupSettings
import org.opensearch.indexmanagement.rollup.util.isRollupIndex
import org.opensearch.indexmanagement.rollup.util.getRollupJobs
import org.opensearch.indexmanagement.rollup.util.changeAggregations
import org.opensearch.indexmanagement.rollup.util.populateFieldMappings
import org.opensearch.indexmanagement.rollup.util.getDateHistogram
import org.opensearch.indexmanagement.rollup.util.rewriteSearchSourceBuilder
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.search.aggregations.AggregationBuilder
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.opensearch.search.internal.ShardSearchRequest
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportChannel
import org.opensearch.transport.TransportInterceptor
import org.opensearch.transport.TransportRequest
import org.opensearch.transport.TransportRequestHandler
@Suppress("TooManyFunctions")
class RollupInterceptor(
    val clusterService: ClusterService,
    val settings: Settings,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
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
    // Returns Pair<containsRollup: Boolean, rollupJob: RollupJob>
    @Suppress("SpreadOperator")
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
    // Returns true if request was already modified into "interceptor_interval_data" bucket aggregation
    fun isRequestRewrittenIntoBuckets(request: ShardSearchRequest): Boolean {
        val currentAggs = request.source().aggregations().aggregatorFactories
        if (currentAggs != null) {
            for (agg in currentAggs) {
                if (agg.name == "interceptor_interval_data") {
                    return true
                }
            }
        }
        return false
    }
    // Helper fn to avoid rewritting a rollup request an extra time
    fun isReqeustRollupFormat(request: ShardSearchRequest): Boolean {
        if (request.source().query() != null) {
            val jsonRequest: String = request.source().query().toString()
            // Detected dummy field from internal search request
            if (jsonRequest.contains("rollup._id")) {
                return true
            }
        }
        return false
    }
    // If the request has a sort on it, size can be > 0 on a rollup search
    fun canHaveSize(request: ShardSearchRequest): Boolean {
        return request.source().sorts() != null
    }
    // Need to modify aggs for rollup docs with avg and value count aggs
    fun modifyRollupAggs(aggFacts: MutableCollection<AggregationBuilder>): AggregatorFactories.Builder {
        val build = AggregatorFactories.builder()
        for (agg in aggFacts) {
            when (agg) {
                is SumAggregationBuilder -> {
                    build.addAggregator(agg)
                }
                is MaxAggregationBuilder -> {
                    build.addAggregator(agg)
                }

                is MinAggregationBuilder -> {
                    build.addAggregator(agg)
                }

                is ValueCountAggregationBuilder -> {
                    // I want to append .rollup.value_count to the name so its identified in response interceptor
                    val newValueCount = ValueCountAggregationBuilder("${agg.name}.rollup.value_count")
                    newValueCount.field(agg.field())
                    build.addAggregator(newValueCount)
                }
                is AvgAggregationBuilder -> {
                    // Going to split this into a value_count and a sum agg to put together in response interceptor
                    // Need to do this since .count and .sum are private in InternalAvg
                    val avgValueCount = ValueCountAggregationBuilder("${agg.name}.rollup.avg.value_count")
                    avgValueCount.field(agg.field())
                    build.addAggregator(avgValueCount)
                    val avgSumCount = SumAggregationBuilder("${agg.name}.rollup.avg.sum")
                    avgSumCount.field(agg.field())
                    build.addAggregator(avgSumCount)
                }

                else -> throw IllegalArgumentException("The ${agg.type} aggregation is not currently supported in rollups")
            }
        }
        return build
    }

    // Wrap original aggregations into buckets based on fixed interval to remove overlap in response interceptor
    fun breakRequestIntoBuckets(request: ShardSearchRequest, rollupJob: Rollup) {
        val oldAggs = modifyRollupAggs(request.source().aggregations().aggregatorFactories)
        var dateSourceField: String = ""
        var rollupInterval: String = ""
        for (dim in rollupJob.dimensions) {
            if (dim is DateHistogram) {
                dateSourceField = dim.sourceField
                rollupInterval = dim.fixedInterval!!
                break
            }
        }
        // Wraps all existing aggs in bucket aggregation
        // Notifies the response interceptor that was rewritten since agg name is interceptor_interval_data
        // Edge case if User selected this as the aggregation name :/
        val intervalAgg = AggregationBuilders.dateHistogram("interceptor_interval_data")
            .field(dateSourceField)
            .calendarInterval(DateHistogramInterval(rollupInterval))
            .format("epoch_millis")
            .subAggregations(oldAggs)
        // Changes aggregation in source to new agg
        request.source(request.source().changeAggregations(listOf(intervalAgg)))
        return
    }

    @Suppress("SpreadOperator", "NestedBlockDepth")
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
                    val shardRequestIndex = request.shardId().indexName
                    val isRollupIndex = isRollupIndex(shardRequestIndex, clusterService.state())
                    // Only modifies rollup searches and avoids internal client calls
                    if (containsRollup || isRollupIndex) {
                        val (concreteRollupIndicesArray, concreteLiveIndicesArray) = getConcreteIndices(request)
                        // Avoid infinite interceptor loop
                        val isMultiSearch = (concreteRollupIndicesArray.isNotEmpty() && concreteLiveIndicesArray.isNotEmpty())
                        if (isMultiSearch && request.source().aggregations() != null && !isRequestRewrittenIntoBuckets(request)) {
                            // Break apart request to remove overlapping parts
                            breakRequestIntoBuckets(request, rollupJob!!)
                        }
                        // Rewrite the request to fit rollup format if not already done previously
                        if (isRollupIndex && !isReqeustRollupFormat(request)) {
                            /* Client calls from the response interceptor require request bodies of 1,
                            otherwise do not allow size > 0 for rollup indices
                             */
                            if (!canHaveSize(request) && request.source().size() != 0) {
                                throw IllegalArgumentException(
                                    "Rollup search must have size explicitly set to 0, " +
                                        "but found ${request.source().size()}"
                                )
                            }
                            rewriteRollupRequest(request, rollupJob!!, concreteRollupIndicesArray)
                        }
                    }
                }
                actualHandler.messageReceived(request, channel, task)
            }
        }
    }
    // Returns Pair (concreteRollupIndices: Array<String>, concreteLiveIndicesArray: Array<String>)
    @Suppress("SpreadOperator")
    fun getConcreteIndices(request: ShardSearchRequest): Pair<Array<String>, Array<String>> {
        val indices = request.indices().map { it.toString() }.toTypedArray()
        val concreteIndices = indexNameExpressionResolver
            .concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
        val concreteRollupIndexNames = mutableListOf<String>()
        val concreteLiveIndexNames = mutableListOf<String>()
        for (indexName in concreteIndices) {
            if (isRollupIndex(indexName, clusterService.state())) {
                concreteRollupIndexNames.add(indexName)
            } else {
                concreteLiveIndexNames.add(indexName)
            }
        }
        val concreteRollupIndicesArray = concreteRollupIndexNames.toTypedArray()
        val concreteLiveIndicesArray = concreteLiveIndexNames.toTypedArray()
        return Pair(concreteRollupIndicesArray, concreteLiveIndicesArray)
    }
    fun rewriteRollupRequest(request: ShardSearchRequest, rollupJob: Rollup, concreteRollupIndicesArray: Array<String>) {
        // To extract fields from QueryStringQueryBuilder we need concrete source index name.
        val queryFieldMappings = getQueryMetadata(
            request.source().query(),
            getConcreteSourceIndex(rollupJob.sourceIndex, indexNameExpressionResolver, clusterService.state())
        )
        val aggregationFieldMappings = getAggregationMetadata(request.source().aggregations()?.aggregatorFactories)
        val fieldMappings = queryFieldMappings + aggregationFieldMappings

        val allMatchingRollupJobs = validateIndicies(concreteRollupIndicesArray, fieldMappings)

        // only rebuild if there is necessity to rebuild
        if (fieldMappings.isNotEmpty()) {
            rewriteShardSearchForRollupJobs(request, allMatchingRollupJobs)
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
                ?: throw IllegalArgumentException("Not all indices have rollup job, missing on $concreteIndex")

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
