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
import org.opensearch.index.query.MatchNoneQueryBuilder
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
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder
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

@Suppress("TooManyFunctions")
class RollupInterceptor(
    val clusterService: ClusterService,
    val settings: Settings,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
) : TransportInterceptor {
    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var searchEnabled = RollupSettings.ROLLUP_SEARCH_ENABLED.get(settings)

    @Volatile private var searchAllJobs = RollupSettings.ROLLUP_SEARCH_ALL_JOBS.get(settings)

    @Volatile private var searchRawRollupIndices = RollupSettings.ROLLUP_SEARCH_SOURCE_INDICES.get(settings)

    companion object {
        /**
         * Bypass level that skips all rollup search validations and rewriting.
         * Used when the system needs to query rollup indices directly using composite aggregation.
         */
        const val BYPASS_ROLLUP_SEARCH = 1

        /**
         * Bypass level that allows non-zero size in rollup searches.
         * Used for internal operations like continuous rollup initialization that need to fetch
         * actual documents from rollup indices (e.g., getEarliestTimestampFromRollupIndex).
         */
        const val BYPASS_SIZE_CHECK = 2

        /**
         * Marker prefix used in FetchSourceContext includes array to communicate bypass levels
         * across nodes in a multi-node cluster. The full marker format is:
         * "${BYPASS_MARKER_PREFIX}<level>" where <level> is an integer (BYPASS_ROLLUP_SEARCH or BYPASS_SIZE_CHECK).
         */
        const val BYPASS_MARKER_PREFIX = "_rollup_internal_bypass_"

        /**
         * Sentinel field name used to neutralize a rollup shard in a mixed raw + rollup search. It is
         * intentionally chosen to never exist on any index so that aggregations referencing it resolve
         * to empty (unmapped) results.
         */
        @Suppress("MemberVisibilityCanBePrivate") // internal for unit testing
        const val NONEXISTENT_ROLLUP_FIELD = "__opensearch_rollup_nonexistent_field__"
    }

    /**
     * Reads the bypass value from the request's FetchSourceContext.
     *
     * The bypass mechanism uses a special marker string in the FetchSourceContext includes array
     * to communicate bypass levels across nodes in a multi-node cluster. The marker format is:
     * "${BYPASS_MARKER_PREFIX}<level>" where <level> is an integer (BYPASS_ROLLUP_SEARCH or BYPASS_SIZE_CHECK).
     *
     * This marker is set by internal components before making search requests:
     * - RollupSearchService: Sets BYPASS_ROLLUP_SEARCH when querying rollup indices with composite aggregations
     *   during the rollup process (multi-tier rollup scenario where a rollup index is the source)
     * - RollupMetadataService: Sets BYPASS_SIZE_CHECK when fetching the earliest timestamp document from
     *   a rollup index during continuous rollup initialization (needs size=1 to retrieve actual documents)
     *
     * The marker is placed in the FetchSourceContext includes array because:
     * 1. It's serialized and transmitted across nodes in the cluster
     * 2. It doesn't affect the actual source fetching behavior (fetchSource is set to false)
     * 3. It provides a clean way to pass metadata without modifying the core search request structure
     *
     * @param request The shard search request to check for bypass markers
     * @return The bypass level if the marker is present in includes array, null otherwise
     */
    internal fun getBypassFromFetchSource(request: ShardSearchRequest): Int? {
        val includes = request.source()?.fetchSource()?.includes()

        // Look for our bypass marker in the includes array and extract the bypass level
        return includes
            ?.find { it.startsWith(BYPASS_MARKER_PREFIX) }
            ?.substringAfter(BYPASS_MARKER_PREFIX)
            ?.toIntOrNull()
    }

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(RollupSettings.ROLLUP_SEARCH_ENABLED) {
            searchEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(RollupSettings.ROLLUP_SEARCH_ALL_JOBS) {
            searchAllJobs = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(RollupSettings.ROLLUP_SEARCH_SOURCE_INDICES) {
            searchRawRollupIndices = it
        }
    }

    @Suppress("SpreadOperator")
    override fun <T : TransportRequest> interceptHandler(
        action: String,
        executor: String,
        forceExecution: Boolean,
        actualHandler: TransportRequestHandler<T>,
    ): TransportRequestHandler<T> = object : TransportRequestHandler<T> {
        override fun messageReceived(request: T, channel: TransportChannel, task: Task) {
            if (searchEnabled && request is ShardSearchRequest) {
                val index = request.shardId().indexName
                val isRollupIndex = isRollupIndex(index, clusterService.state())
                if (isRollupIndex) {
                    // Check bypass from FetchSourceContext
                    val bypassLevel = getBypassFromFetchSource(request)

                    logger.debug("RollupInterceptor bypass check - bypassLevel: $bypassLevel")

                    // BYPASS_ROLLUP_SEARCH: Skip all validations and query rewriting
                    // Used for composite aggregation queries when rolling up rollup indices
                    if (bypassLevel == BYPASS_ROLLUP_SEARCH) {
                        actualHandler.messageReceived(request, channel, task)
                        return
                    }

                    // BYPASS_SIZE_CHECK: Allow non-zero size for internal operations that need to
                    // fetch documents (e.g., fetching earliest timestamp document for continuous rollup initialization)
                    // Normal rollup searches must have size=0 since they should only return aggregations
                    if (bypassLevel != BYPASS_SIZE_CHECK && request.source().size() != 0) {
                        throw IllegalArgumentException("Rollup search must have size explicitly set to 0, but found ${request.source().size()}")
                    }

                    val indices = request.indices().map { it.toString() }.toTypedArray()
                    val concreteIndices =
                        indexNameExpressionResolver
                            .concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
                    // To extract fields from QueryStringQueryBuilder we need concrete source index name.
                    val rollupJob =
                        clusterService.state().metadata.index(index).getRollupJobs()?.get(0)
                            ?: throw IllegalArgumentException("No rollup job associated with target_index")
                    val queryFieldMappings =
                        getQueryMetadata(
                            request.source().query(),
                            getConcreteSourceIndex(rollupJob.sourceIndex, indexNameExpressionResolver, clusterService.state()),
                        )
                    val aggregationFieldMappings = getAggregationMetadata(request.source().aggregations()?.aggregatorFactories)
                    val fieldMappings = queryFieldMappings + aggregationFieldMappings

                    val allMatchingRollupJobs = validateIndicies(concreteIndices, fieldMappings)

                    if (fieldMappings.isNotEmpty() && allMatchingRollupJobs.isEmpty()) {
                        // Mixed raw + rollup search where no rollup index can answer the queried fields
                        // (e.g. the field exists only on the raw index). Neutralize this rollup shard so it
                        // matches no documents and returns empty aggregations of the same shape as the request,
                        // letting the raw indices serve the query instead of failing the entire search.
                        neutralizeRollupShard(request)
                    } else if (fieldMappings.isNotEmpty()) {
                        // only rebuild if there is necessity to rebuild
                        rewriteShardSearchForRollupJobs(request, allMatchingRollupJobs)
                    }
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
     * Validate that at least one index has a rollup job which matches field mappings from request.
     * Indices whose rollup jobs don't cover the queried fields are skipped, allowing queries across
     * multiple rollup indices with different dimension schemas.
     *
     * Returns the matching rollup jobs. The result may be empty when the search also spans raw
     * (non-rollup) indices and no rollup index can answer the queried fields (for example, the field
     * exists only on the raw index). In that case the rollup shard should contribute nothing and let
     * the raw indices serve the query, instead of failing the entire search. The caller is responsible
     * for neutralizing the rollup shard when this returns an empty map.
     * */
    internal fun validateIndicies(concreteIndices: Array<String>, fieldMappings: Set<RollupFieldMapping>): Map<Rollup, Set<RollupFieldMapping>> {
        var allMatchingRollupJobs: Map<Rollup, Set<RollupFieldMapping>> = mapOf()
        var lastIssues: Set<String> = emptySet()
        var hasRawIndex = false
        for (concreteIndex in concreteIndices) {
            val rollupJobs = clusterService.state().metadata.index(concreteIndex).getRollupJobs()
            if (rollupJobs != null) {
                val (matchingRollupJobs, issues) = findMatchingRollupJobs(fieldMappings, rollupJobs)
                if (matchingRollupJobs.isNotEmpty()) {
                    allMatchingRollupJobs += matchingRollupJobs
                } else {
                    lastIssues = issues
                    logger.debug("Skipping rollup index [$concreteIndex] as it does not match query fields: $issues")
                }
            } else if (!searchRawRollupIndices) {
                throw IllegalArgumentException("Not all indices have rollup job")
            } else {
                // A raw (non-rollup) index is part of a mixed raw + rollup search.
                hasRawIndex = true
            }
        }

        // Only fail the query when there is no raw index that could answer it. When raw indices are
        // present, an empty result signals the caller to neutralize this rollup shard so the raw
        // indices can serve the query (treating fields missing from the rollup as no contribution).
        if (allMatchingRollupJobs.isEmpty() && !hasRawIndex) {
            throw IllegalArgumentException("Could not find a rollup job that can answer this query because $lastIssues")
        }

        return allMatchingRollupJobs
    }

    @Suppress("CyclomaticComplexMethod")
    private fun getAggregationMetadata(
        aggregationBuilders: Collection<AggregationBuilder>?,
        fieldMappings: MutableSet<RollupFieldMapping> = mutableSetOf(),
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

                is CardinalityAggregationBuilder -> {
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

    @Suppress("CyclomaticComplexMethod", "ThrowsCount", "LongMethod")
    private fun getQueryMetadata(
        query: QueryBuilder?,
        concreteSourceIndexName: String?,
        fieldMappings: MutableSet<RollupFieldMapping> = mutableSetOf(),
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
                if (!query.analyzer().isNullOrEmpty() ||
                    query.slop() != MatchQuery.DEFAULT_PHRASE_SLOP ||
                    query.zeroTermsQuery() != MatchQuery.DEFAULT_ZERO_TERMS_QUERY
                ) {
                    throw IllegalArgumentException(
                        "The ${query.name} query is currently not supported with analyzer/slop/zero_terms_query in rollups",
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
    @Suppress("CyclomaticComplexMethod")
    internal fun findMatchingRollupJobs(
        fieldMappings: Set<RollupFieldMapping>,
        rollupJobs: List<Rollup>,
    ): Pair<Map<Rollup, Set<RollupFieldMapping>>, Set<String>> {
        val rollupFieldMappings =
            rollupJobs.map { rollup ->
                rollup to rollup.populateFieldMappings()
            }.toMap()

        val knownFieldMappings = mutableSetOf<RollupFieldMapping>()
        val unknownFields = mutableSetOf<String>()

        fieldMappings.forEach {
            if (it.mappingType == UNKNOWN_MAPPING) {
                unknownFields.add(it.fieldName)
            } else {
                knownFieldMappings.add(it)
            }
        }

        val potentialRollupFieldMappings =
            rollupFieldMappings.filterValues {
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
                if (!allFields.contains(it.fieldName)) {
                    issues.add(it.toIssue(true))
                } else if (it.mappingType != UNKNOWN_MAPPING && !allFieldMappings.contains(it)) {
                    issues.add(it.toIssue())
                }
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
            if (getEstimateRollupInterval(matched) > getEstimateRollupInterval(new)) {
                matched
            } else {
                new
            }
        }
    }

    private fun getEstimateRollupInterval(rollup: Rollup): Long = if (rollup.getDateHistogram().calendarInterval != null) {
        DateHistogramInterval(rollup.getDateHistogram().calendarInterval).estimateMillis()
    } else {
        DateHistogramInterval(rollup.getDateHistogram().fixedInterval).estimateMillis()
    }

    /**
     * Builds a copy of [aggregationBuilder] that references a field which does not exist on the rollup
     * index, preserving the aggregation name, type, and sub-aggregations. Running the copy against the
     * rollup index yields an empty (unmapped) result of the same aggregation type that the raw indices
     * produce, so the coordinator can reduce this rollup shard's empty contribution together with the
     * raw shards' results. For bucketing aggregations (date_histogram, histogram) every setting that
     * influences bucket boundaries and empty-bucket generation (interval, bounds, offset, min_doc_count,
     * order, keyed and time_zone) must be copied verbatim; otherwise the neutralized shard rounds buckets
     * differently from the raw shards and the coordinator's empty-bucket reduce produces misaligned or
     * out-of-order keys, corrupting results or fatally tripping InternalDateHistogram.addEmptyBuckets.
     */
    @Suppress("CyclomaticComplexMethod", "LongMethod")
    internal fun neutralizeAggregation(aggregationBuilder: AggregationBuilder): AggregationBuilder {
        val neutralized: AggregationBuilder =
            when (aggregationBuilder) {
                is TermsAggregationBuilder -> TermsAggregationBuilder(aggregationBuilder.name).field(NONEXISTENT_ROLLUP_FIELD)

                is DateHistogramAggregationBuilder -> {
                    // Preserve every bucketing setting (interval, bounds, offset, min_doc_count, order, keyed and
                    // especially time_zone) so the empty result this shard produces uses the exact same bucket
                    // boundaries and rounding as the raw shards. Otherwise the coordinator's empty-bucket reduce
                    // can generate out-of-order/misaligned bucket keys, which corrupts results or trips a fatal
                    // assertion during InternalDateHistogram.addEmptyBuckets.
                    DateHistogramAggregationBuilder(aggregationBuilder.name)
                        .also { aggregationBuilder.calendarInterval?.apply { it.calendarInterval(this) } }
                        .also { aggregationBuilder.fixedInterval?.apply { it.fixedInterval(this) } }
                        .also { aggregationBuilder.extendedBounds()?.apply { it.extendedBounds(this) } }
                        .keyed(aggregationBuilder.keyed())
                        .also { if (aggregationBuilder.minDocCount() >= 0) it.minDocCount(aggregationBuilder.minDocCount()) }
                        .offset(aggregationBuilder.offset())
                        .also { aggregationBuilder.order()?.apply { it.order(this) } }
                        .also { aggregationBuilder.timeZone()?.apply { it.timeZone(this) } }
                        .field(NONEXISTENT_ROLLUP_FIELD)
                }

                is HistogramAggregationBuilder ->
                    // Preserve the interval, bounds, offset, min_doc_count, order and keyed for the same reason as
                    // date_histogram above.
                    HistogramAggregationBuilder(aggregationBuilder.name)
                        .interval(aggregationBuilder.interval())
                        .also {
                            if (aggregationBuilder.minBound().isFinite() && aggregationBuilder.maxBound().isFinite()) {
                                it.extendedBounds(aggregationBuilder.minBound(), aggregationBuilder.maxBound())
                            }
                        }
                        .keyed(aggregationBuilder.keyed())
                        .also { if (aggregationBuilder.minDocCount() >= 0) it.minDocCount(aggregationBuilder.minDocCount()) }
                        .offset(aggregationBuilder.offset())
                        .also { aggregationBuilder.order()?.apply { it.order(this) } }
                        .field(NONEXISTENT_ROLLUP_FIELD)

                is SumAggregationBuilder -> SumAggregationBuilder(aggregationBuilder.name).field(NONEXISTENT_ROLLUP_FIELD)

                is AvgAggregationBuilder -> AvgAggregationBuilder(aggregationBuilder.name).field(NONEXISTENT_ROLLUP_FIELD)

                is MaxAggregationBuilder -> MaxAggregationBuilder(aggregationBuilder.name).field(NONEXISTENT_ROLLUP_FIELD)

                is MinAggregationBuilder -> MinAggregationBuilder(aggregationBuilder.name).field(NONEXISTENT_ROLLUP_FIELD)

                is ValueCountAggregationBuilder -> ValueCountAggregationBuilder(aggregationBuilder.name).field(NONEXISTENT_ROLLUP_FIELD)

                is CardinalityAggregationBuilder -> CardinalityAggregationBuilder(aggregationBuilder.name).field(NONEXISTENT_ROLLUP_FIELD)

                else -> throw IllegalArgumentException("The ${aggregationBuilder.type} aggregation is not currently supported in rollups")
            }
        aggregationBuilder.subAggregations.forEach { neutralized.subAggregation(neutralizeAggregation(it)) }
        return neutralized
    }

    /**
     * Neutralizes a rollup shard for a mixed raw + rollup search whose queried fields are not covered by
     * any rollup job on this index. The shard is rewritten to match no documents and to return empty
     * aggregations of the same shape as the request, so that the raw indices in the search can serve the
     * query instead of the whole search failing.
     */
    private fun neutralizeRollupShard(request: ShardSearchRequest) {
        val neutralized = SearchSourceBuilder().size(0).query(MatchNoneQueryBuilder())
        request.source().aggregations()?.aggregatorFactories?.forEach {
            neutralized.aggregation(neutralizeAggregation(it))
        }
        request.source(neutralized)
    }

    private fun rewriteShardSearchForRollupJobs(request: ShardSearchRequest, matchingRollupJobs: Map<Rollup, Set<RollupFieldMapping>>) {
        val matchedRollup = pickRollupJob(matchingRollupJobs.keys)
        val fieldNameMappingTypeMap = matchingRollupJobs.getValue(matchedRollup).associateBy({ it.fieldName }, { it.mappingType })
        val concreteSourceIndex = getConcreteSourceIndex(matchedRollup.sourceIndex, indexNameExpressionResolver, clusterService.state())
        if (searchAllJobs) {
            request.source(request.source().rewriteSearchSourceBuilder(matchingRollupJobs.keys, fieldNameMappingTypeMap, concreteSourceIndex))
        } else {
            if (matchingRollupJobs.keys.size > 1) {
                logger.trace("Trying search with search across multiple rollup jobs disabled so will give result with largest rollup window")
            }
            request.source(request.source().rewriteSearchSourceBuilder(matchedRollup, fieldNameMappingTypeMap, concreteSourceIndex))
        }
    }
}
