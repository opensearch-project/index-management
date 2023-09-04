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
import org.opensearch.index.search.MatchQuery
import org.opensearch.indexmanagement.common.model.dimension.Dimension
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
import org.opensearch.search.internal.ShardSearchRequest
import org.opensearch.transport.TransportInterceptor
import org.opensearch.transport.TransportRequest
import org.opensearch.transport.TransportRequestHandler

/**
 *
 */
class RollupInterceptor(
    val clusterService: ClusterService,
    val settings: Settings,
    val indexNameExpressionResolver: IndexNameExpressionResolver
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

    @Suppress("SpreadOperator")
    override fun <T : TransportRequest> interceptHandler(
        action: String,
        executor: String,
        forceExecution: Boolean,
        actualHandler: TransportRequestHandler<T>
    ): TransportRequestHandler<T> {
        return TransportRequestHandler<T> { request, channel, task ->
            if (searchEnabled && request is ShardSearchRequest) {
                val index = request.shardId().indexName
                if (isRollupIndex(index, clusterService.state())) {
                    if (request.source().size() != 0) {
                        throw IllegalArgumentException("Rollup search must have size explicitly set to 0, but found ${request.source().size()}")
                    }

                    val indices = request.indices().map { it.toString() }.toTypedArray()
                    val concreteIndices = indexNameExpressionResolver
                        .concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
                    // To extract fields from QueryStringQueryBuilder we need concrete source index name.
                    val rollupJob = clusterService.state().metadata.index(index).getRollupJobs()?.get(0)
                        ?: throw IllegalArgumentException("No rollup job associated with target_index")
                    val queryFieldMappings = getFieldMappingFromQuery(
                        request.source().query(),
                        getConcreteIndex(rollupJob.sourceIndex, indexNameExpressionResolver, clusterService.state())
                    )
                    val aggregationFieldMappings = getFieldMappingFromAgg(request.source().aggregations()?.aggregatorFactories)
                    val fieldMappings = queryFieldMappings + aggregationFieldMappings
                    val allMatchingRollupJobs = validateIndicies(concreteIndices, fieldMappings)
                    if (fieldMappings.isNotEmpty()) {
                        rewriteShardSearchForRollupJobs(request, allMatchingRollupJobs)
                    }
                }
            }
            actualHandler.messageReceived(request, channel, task)
        }
    }

    /**
     * Get the newest concrete index
     */
    private fun getConcreteIndex(sourceIndex: String, resolver: IndexNameExpressionResolver, clusterState: ClusterState): String {
        val concreteIndexNames = resolver.concreteIndexNames(clusterState, IndicesOptions.LENIENT_EXPAND_OPEN, sourceIndex)
        if (concreteIndexNames.isEmpty()) {
            logger.warn("Cannot resolve rollup sourceIndex [$sourceIndex]")
            return ""
        }

        var concreteIndexName = ""
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

    /**
     * Validate that all indices have rollup job which matches field mappings from request
     */
    @Suppress("ThrowsCount")
    private fun validateIndicies(concreteIndices: Array<String>, fieldMappings: Set<RollupFieldMapping>): Map<Rollup, Set<RollupFieldMapping>> {
        val allMatchingRollupJobs: MutableMap<Rollup, Set<RollupFieldMapping>> = mutableMapOf()
        val allIssues: MutableSet<String> = mutableSetOf()
        for (concreteIndex in concreteIndices) {
            val rollupJobs = clusterService.state().metadata.index(concreteIndex).getRollupJobs()
                ?: throw IllegalArgumentException("Not all indices have rollup job")
            val (matchingRollupJobs, issues) = findMatchingRollupJobs(fieldMappings, rollupJobs)
            allMatchingRollupJobs += matchingRollupJobs
            allIssues += issues
        }
        if (allIssues.isNotEmpty()) {
            throw IllegalArgumentException("Failed to validate the indices of the rollup query because: $allIssues")
        }
        if (allMatchingRollupJobs.isEmpty()) {
            throw IllegalArgumentException("Failed to find any matching rollup jobs for the indices [$concreteIndices]")
        }
        return allMatchingRollupJobs
    }

    @Suppress("ComplexMethod")
    private fun getFieldMappingFromAgg(
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
                getFieldMappingFromAgg(it.subAggregations, fieldMappings)
            }
        }
        return fieldMappings
    }

    @Suppress("ComplexMethod", "ThrowsCount", "LongMethod")
    private fun getFieldMappingFromQuery(
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
                // TODO why unknown mapping here?
                fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, query.fieldName(), UNKNOWN_MAPPING))
            }
            is MatchAllQueryBuilder -> {
                // do nothing
            }
            is BoolQueryBuilder -> {
                query.must()?.forEach { this.getFieldMappingFromQuery(it, concreteSourceIndexName, fieldMappings) }
                query.mustNot()?.forEach { this.getFieldMappingFromQuery(it, concreteSourceIndexName, fieldMappings) }
                query.should()?.forEach { this.getFieldMappingFromQuery(it, concreteSourceIndexName, fieldMappings) }
                query.filter()?.forEach { this.getFieldMappingFromQuery(it, concreteSourceIndexName, fieldMappings) }
            }
            is BoostingQueryBuilder -> {
                this.getFieldMappingFromQuery(query.positiveQuery(), concreteSourceIndexName, fieldMappings)
                this.getFieldMappingFromQuery(query.negativeQuery(), concreteSourceIndexName, fieldMappings)
            }
            is ConstantScoreQueryBuilder -> {
                this.getFieldMappingFromQuery(query.innerQuery(), concreteSourceIndexName, fieldMappings)
            }
            is DisMaxQueryBuilder -> {
                query.innerQueries().forEach { this.getFieldMappingFromQuery(it, concreteSourceIndexName, fieldMappings) }
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
    /**
     * @return matching rollups or issues
     */
    @Suppress("ComplexMethod")
    private fun findMatchingRollupJobs(
        fieldMappings: Set<RollupFieldMapping>,
        rollups: List<Rollup>
    ): Pair<Map<Rollup, Set<RollupFieldMapping>>, Set<String>> {
        val rollupFieldMappings = rollups.associateWith { rollup -> rollup.populateFieldMappings() }

        val knownFieldMappings = mutableSetOf<RollupFieldMapping>()
        val unknownFields = mutableSetOf<String>()
        fieldMappings.forEach {
            if (it.mappingType == UNKNOWN_MAPPING) unknownFields.add(it.fieldName)
            else knownFieldMappings.add(it)
        }

        val matchingRollupFieldMappings = rollupFieldMappings.filterValues {
            it.containsAll(knownFieldMappings) &&
                it.map { rollupFieldMapping -> rollupFieldMapping.fieldName }.containsAll(unknownFields)
        }

        val issues = mutableSetOf<String>()
        if (matchingRollupFieldMappings.isEmpty()) {
            // create a global set of all field mappings
            val allFieldMappings = mutableSetOf<RollupFieldMapping>()
            rollupFieldMappings.values.forEach { allFieldMappings.addAll(it) }
            val allFieldNames = allFieldMappings.map { it.fieldName }
            // Adding to the issue if cannot find defined field mapping or if the field is missing
            fieldMappings.forEach {
                if (!allFieldNames.contains(it.fieldName)) issues.add(it.toIssue(true))
                else if (it.mappingType != UNKNOWN_MAPPING && !allFieldMappings.contains(it)) issues.add(it.toIssue())
            }
        }

        return matchingRollupFieldMappings to issues
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
        val rollup = pickRollupJob(matchingRollupJobs.keys)
        val fieldNameMappingTypeMap = matchingRollupJobs.getValue(rollup).associateBy({ it.fieldName }, { it.mappingType })
        val concreteSourceIndex = getConcreteIndex(rollup.sourceIndex, indexNameExpressionResolver, clusterService.state())
        if (searchAllJobs) {
            request.source(request.source().rewriteSearchSourceBuilder(matchingRollupJobs.keys, fieldNameMappingTypeMap, concreteSourceIndex))
        } else {
            request.source(request.source().rewriteSearchSourceBuilder(rollup, fieldNameMappingTypeMap, concreteSourceIndex))
        }
    }
}
