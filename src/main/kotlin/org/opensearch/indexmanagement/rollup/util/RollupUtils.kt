/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:Suppress("TooManyFunctions")

package org.opensearch.indexmanagement.rollup.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
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
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.common.model.dimension.Histogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.rollup.RollupMapperService
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupFieldMapping
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Cardinality
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.model.metric.ValueCount
import org.opensearch.indexmanagement.rollup.query.QueryStringQueryUtil
import org.opensearch.indexmanagement.rollup.settings.LegacyOpenDistroRollupSettings
import org.opensearch.indexmanagement.rollup.settings.RollupSettings
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.search.aggregations.AggregationBuilder
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder
import org.opensearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder

const val DATE_FIELD_STRICT_DATE_OPTIONAL_TIME_FORMAT = "strict_date_optional_time"
const val DATE_FIELD_EPOCH_MILLIS_FORMAT = "epoch_millis"

private val logger = LogManager.getLogger("RollupUtils")

@Suppress("ReturnCount")
fun isRollupIndex(index: String, clusterState: ClusterState): Boolean {
    val idx = clusterState.metadata.index(index)
    if (idx == null) {
        return false
    } else if (RollupSettings.ROLLUP_INDEX.get(idx.settings)) {
        return true
    } else if (LegacyOpenDistroRollupSettings.ROLLUP_INDEX.get(idx.settings)) {
        return true
    }
    return false
}

fun Rollup.isTargetIndexAlias(): Boolean = RollupFieldValueExpressionResolver.indexAliasUtils.isAlias(targetIndex)

fun Rollup.getRollupSearchRequest(metadata: RollupMetadata, clusterState: ClusterState): SearchRequest {
    // In case of multi-tier rollup the source index will not have raw field stored, so need to handle this for continuous jobs
    val isSourceRollupIndex = isRollupIndex(this.sourceIndex, clusterState)
    val dateHistogram = this.getDateHistogram()
    val dateField = if (isSourceRollupIndex) {
        "${dateHistogram.sourceField}.date_histogram"
    } else {
        dateHistogram.sourceField
    }

    val query =
        if (metadata.continuous != null) {
            RangeQueryBuilder(dateField)
                .from(metadata.continuous.nextWindowStartTime.toEpochMilli(), true)
                .to(metadata.continuous.nextWindowEndTime.toEpochMilli(), false)
                .format(DATE_FIELD_EPOCH_MILLIS_FORMAT)
        } else {
            MatchAllQueryBuilder()
        }
    val searchSourceBuilder =
        SearchSourceBuilder()
            .trackTotalHits(false)
            .size(0)
            .aggregation(this.getCompositeAggregationBuilder(metadata.afterKey, clusterState))
            .query(query)
    return SearchRequest(this.sourceIndex)
        .source(searchSourceBuilder)
        .allowPartialSearchResults(false)
}

/**
 * Builds a composite aggregation for rollup jobs, with support for multi-tier rollups
 *
 * This method handles two scenarios:
 * 1. Rolling up raw data from a regular index
 * 2. Rolling up pre-aggregated data from a rollup index (multi-tier rollup)
 *
 * For multi-tier rollups, the source index is itself a rollup index, which means:
 * - Dimension fields are stored with type suffixes (e.g., "field.date_histogram", "field.terms")
 * - Metric fields are stored as pre-computed aggregations (e.g., "field.sum", "field.min")
 * - We need to aggregate the pre-aggregated values to maintain correctness
 *
 * @param afterKey Optional pagination key for composite aggregation continuation
 * @param clusterState Current cluster state used to determine if source is a rollup index
 * @return CompositeAggregationBuilder configured for either raw data or rollup-on-rollup aggregation
 */
@Suppress("CyclomaticComplexMethod", "NestedBlockDepth", "LongMethod")
fun Rollup.getCompositeAggregationBuilder(afterKey: Map<String, Any>?, clusterState: ClusterState): CompositeAggregationBuilder {
    // Determine if the source index is a rollup index to adjust field references accordingly
    val isRollupIndex = isRollupIndex(this.sourceIndex, clusterState)

    return if (isRollupIndex) {
        buildCompositeAggregationForRollupIndex(afterKey)
    } else {
        buildCompositeAggregationForStandardIndex(afterKey)
    }
}

/**
 * Builds composite aggregation for multi-tier rollup (source is a rollup index).
 *
 * In multi-tier rollups:
 * - Dimension fields are stored with type suffixes (e.g., "field.date_histogram", "field.terms")
 * - Metric fields are pre-computed aggregations that need to be re-aggregated
 * - Average is stored as separate sum and value_count fields
 * - ValueCount is stored as a sum that needs to be summed again
 */
private fun Rollup.buildCompositeAggregationForRollupIndex(afterKey: Map<String, Any>?): CompositeAggregationBuilder {
    val sources = mutableListOf<CompositeValuesSourceBuilder<*>>()

    // Build dimension sources with type suffixes
    this.dimensions.forEach { dimension ->
        val sourceBuilder = dimension.toSourceBuilder(appendType = true)
        when (dimension) {
            is DateHistogram -> {
                sourceBuilder.field("${dimension.targetField}.${dimension.type.type}")
            }

            is Terms -> {
                sourceBuilder.field("${dimension.targetField}.${dimension.type.type}")
            }

            is Histogram -> {
                sourceBuilder.field("${dimension.targetField}.${dimension.type.type}")
            }
        }
        sources.add(sourceBuilder)
    }

    return CompositeAggregationBuilder(this.id, sources).size(this.pageSize).also { compositeAgg ->
        afterKey?.let { compositeAgg.aggregateAfter(it) }
        addMetricAggregationsForRollupIndex(compositeAgg)
    }
}

/**
 * Adds metric aggregations for multi-tier rollup to the composite aggregation.
 * Handles re-aggregation of pre-computed metrics from rollup indices.
 */
private fun Rollup.addMetricAggregationsForRollupIndex(compositeAgg: CompositeAggregationBuilder) {
    this.metrics.forEach { metric ->
        val subAggs = metric.metrics.flatMap { agg ->
            when (agg) {
                is Average -> {
                    // Average is stored as separate sum and value_count fields
                    // Sum both components to aggregate across multiple rollup documents
                    listOf(
                        SumAggregationBuilder(metric.targetFieldWithType(agg) + ".sum")
                            .field(metric.targetFieldWithType(agg) + ".sum"),
                        SumAggregationBuilder(metric.targetFieldWithType(agg) + ".value_count")
                            .field(metric.targetFieldWithType(agg) + ".value_count"),
                    )
                }

                is Sum -> {
                    // Sum the pre-computed sum values
                    listOf(
                        SumAggregationBuilder(metric.targetFieldWithType(agg))
                            .field(metric.targetFieldWithType(agg)),
                    )
                }

                is Max -> {
                    // Take max of pre-computed max values (max of maxes)
                    listOf(
                        MaxAggregationBuilder(metric.targetFieldWithType(agg))
                            .field(metric.targetFieldWithType(agg)),
                    )
                }

                is Min -> {
                    // Take min of pre-computed min values (min of mins)
                    listOf(
                        MinAggregationBuilder(metric.targetFieldWithType(agg))
                            .field(metric.targetFieldWithType(agg)),
                    )
                }

                is ValueCount -> {
                    // Sum the pre-computed value_count fields
                    // Each rollup document contains a count of raw documents it represents
                    listOf(
                        SumAggregationBuilder(metric.targetFieldWithType(agg))
                            .field(metric.targetFieldWithType(agg)),
                    )
                }

                is Cardinality -> {
                    // Cardinality aggregation for HLL++ sketches
                    // Multi-tier: Aggregate over pre-computed HLL sketches (field.hll)
                    // Standard: Compute HLL sketch from raw field values
                    listOf(
                        CardinalityAggregationBuilder(metric.targetFieldWithType(agg))
                            .field(metric.targetFieldWithType(agg))
                            .precisionThreshold(agg.precisionThreshold),
                    )
                }

                else -> throw IllegalArgumentException("Found unsupported metric aggregation ${agg.type.type}")
            }
        }
        subAggs.forEach { compositeAgg.subAggregation(it) }
    }
}

/**
 * Builds composite aggregation for standard rollup (source is a regular index with raw data).
 *
 * Uses the original source fields directly and computes aggregations from raw data.
 */
private fun Rollup.buildCompositeAggregationForStandardIndex(afterKey: Map<String, Any>?): CompositeAggregationBuilder {
    val sources = this.dimensions.map { dimension ->
        dimension.toSourceBuilder(appendType = true)
    }

    return CompositeAggregationBuilder(this.id, sources).size(this.pageSize).also { compositeAgg ->
        afterKey?.let { compositeAgg.aggregateAfter(it) }
        addMetricAggregationsForStandardIndex(compositeAgg)
    }
}

/**
 * Adds metric aggregations for standard rollup to the composite aggregation.
 * Computes aggregations from raw data fields.
 */
private fun Rollup.addMetricAggregationsForStandardIndex(compositeAgg: CompositeAggregationBuilder) {
    this.metrics.forEach { metric ->
        val subAggs = metric.metrics.flatMap { agg ->
            when (agg) {
                is Average -> {
                    // Compute sum and count from raw data
                    listOf(
                        SumAggregationBuilder(metric.targetFieldWithType(agg) + ".sum")
                            .field(metric.sourceField),
                        ValueCountAggregationBuilder(metric.targetFieldWithType(agg) + ".value_count")
                            .field(metric.sourceField),
                    )
                }

                is Sum -> {
                    // Sum the raw field values
                    listOf(
                        SumAggregationBuilder(metric.targetFieldWithType(agg))
                            .field(metric.sourceField),
                    )
                }

                is Max -> {
                    // Take max of raw field values
                    listOf(
                        MaxAggregationBuilder(metric.targetFieldWithType(agg))
                            .field(metric.sourceField),
                    )
                }

                is Min -> {
                    // Take min of raw field values
                    listOf(
                        MinAggregationBuilder(metric.targetFieldWithType(agg))
                            .field(metric.sourceField),
                    )
                }

                is ValueCount -> {
                    // Count the raw documents
                    listOf(
                        ValueCountAggregationBuilder(metric.targetFieldWithType(agg))
                            .field(metric.sourceField),
                    )
                }

                is Cardinality -> {
                    // Cardinality aggregation for HLL++ sketches
                    // Multi-tier: Aggregate over pre-computed HLL sketches (field.hll)
                    // Standard: Compute HLL sketch from raw field values
                    listOf(
                        CardinalityAggregationBuilder(metric.targetFieldWithType(agg))
                            .field(metric.sourceField)
                            .precisionThreshold(agg.precisionThreshold),
                    )
                }

                else -> throw IllegalArgumentException("Found unsupported metric aggregation ${agg.type.type}")
            }
        }
        subAggs.forEach { compositeAgg.subAggregation(it) }
    }
}

// There can only be one date histogram in Rollup and it should always be in the first position of dimensions
// This is validated in the rollup init itself, but need to redo it here to correctly return date histogram
fun Rollup.getDateHistogram(): DateHistogram {
    val dimension = this.dimensions.first()
    require(dimension is DateHistogram) { "The first dimension in rollup must be a date histogram" }
    return dimension
}

fun Rollup.findMatchingDimension(field: String, type: Dimension.Type): Dimension? =
    this.dimensions.find { dimension -> dimension.sourceField == field && dimension.type == type }

// This method is only to be used after its confirmed the search/aggs is valid and these exist
@Suppress("NestedBlockDepth")
inline fun <reified T> Rollup.findMatchingMetricField(field: String): String {
    for (rollupMetrics in this.metrics) {
        if (rollupMetrics.sourceField == field) {
            for (metric in rollupMetrics.metrics) {
                if (metric is T) {
                    return rollupMetrics.targetFieldWithType(metric)
                }
            }
        }
    }
    error("Did not find matching rollup metric")
}

@Suppress("NestedBlockDepth", "CyclomaticComplexMethod")
fun IndexMetadata.getRollupJobs(): List<Rollup>? {
    val rollupJobs = mutableListOf<Rollup>()
    val source = this.mapping()?.source() ?: return null
    val xcp =
        XContentHelper
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, source.compressedReference(), XContentType.JSON)
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp) // start of block
    ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp) // _doc
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp) // start of _doc block
    while (xcp.nextToken() != Token.END_OBJECT) {
        val fieldName = xcp.currentName()
        xcp.nextToken()

        when (fieldName) {
            IndexUtils._META -> {
                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val metaField = xcp.currentName()
                    xcp.nextToken()

                    when (metaField) {
                        RollupMapperService.ROLLUPS -> {
                            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                            while (xcp.nextToken() != Token.END_OBJECT) {
                                val rollupID = xcp.currentName()
                                xcp.nextToken()
                                rollupJobs.add(Rollup.parse(xcp, rollupID))
                            }
                        }

                        else -> xcp.skipChildren()
                    }
                }
            }

            else -> xcp.skipChildren()
        }
    }
    // This method is the authoritative source for getting the rollupJobs for an index and if there are none found this method should return null
    return if (rollupJobs.size > 0) rollupJobs else null
}

// TODO: If we have to set this manually for each aggregation builder then it means we could miss new ones settings in the future
@Suppress("CyclomaticComplexMethod", "LongMethod")
fun Rollup.rewriteAggregationBuilder(aggregationBuilder: AggregationBuilder): AggregationBuilder {
    val aggFactory =
        AggregatorFactories.builder().also { factories ->
            aggregationBuilder.subAggregations.forEach {
                factories.addAggregator(this.rewriteAggregationBuilder(it))
            }
        }

    return when (aggregationBuilder) {
        is TermsAggregationBuilder -> {
            val dim = this.findMatchingDimension(aggregationBuilder.field(), Dimension.Type.TERMS) as Terms
            dim.getRewrittenAggregation(aggregationBuilder, aggFactory)
        }

        is DateHistogramAggregationBuilder -> {
            val dim = this.findMatchingDimension(aggregationBuilder.field(), Dimension.Type.DATE_HISTOGRAM) as DateHistogram
            dim.getRewrittenAggregation(aggregationBuilder, aggFactory)
        }

        is HistogramAggregationBuilder -> {
            val dim = this.findMatchingDimension(aggregationBuilder.field(), Dimension.Type.HISTOGRAM) as Histogram
            dim.getRewrittenAggregation(aggregationBuilder, aggFactory)
        }

        is SumAggregationBuilder -> {
            SumAggregationBuilder(aggregationBuilder.name)
                .field(this.findMatchingMetricField<Sum>(aggregationBuilder.field()))
        }

        is AvgAggregationBuilder -> {
            ScriptedMetricAggregationBuilder(aggregationBuilder.name)
                .initScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "state.sums = 0D; state.counts = 0L;", emptyMap()))
                .mapScript(
                    Script(
                        ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                        "state.sums += doc[\"${this.findMatchingMetricField<Average>(aggregationBuilder.field()) + ".sum"}\"].value; " +
                            "state.counts += doc[\"${this.findMatchingMetricField<Average>(aggregationBuilder.field()) + ".value_count"}\"" +
                            "].value",
                        emptyMap(),
                    ),
                )
                .combineScript(
                    Script(
                        ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                        "def d = new org.opensearch.search.aggregations.metrics.ScriptedAvg(state.sums, state.counts); return d", emptyMap(),
                    ),
                )
                .reduceScript(
                    Script(
                        ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                        "double sum = 0; double count = 0; for (a in states) { sum += a.getSum(); count += a.getCount(); } return sum/count", emptyMap(),
                    ),
                )
        }

        is MaxAggregationBuilder -> {
            MaxAggregationBuilder(aggregationBuilder.name)
                .field(this.findMatchingMetricField<Max>(aggregationBuilder.field()))
        }

        is MinAggregationBuilder -> {
            MinAggregationBuilder(aggregationBuilder.name)
                .field(this.findMatchingMetricField<Min>(aggregationBuilder.field()))
        }

        is ValueCountAggregationBuilder -> {
            /*
             * A value count aggs of a pre-computed value count is incorrect as it just returns the number of
             * pre-computed value counts instead of their sum. Unfortunately can't just use the sum aggregation
             * because I was not able to find a way to cast the result of that to a long (instead of the returned float)
             * and the 3893 vs 3893.0 was bothering me.. so this is the next best I can think of. Hopefully there is a better
             * way and we can use that in the future.
             * */
            ScriptedMetricAggregationBuilder(aggregationBuilder.name)
                .initScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "state.valueCounts = []", emptyMap()))
                .mapScript(
                    Script(
                        ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                        "state.valueCounts.add(doc[\"${this.findMatchingMetricField<ValueCount>(aggregationBuilder.field())}\"].value)",
                        emptyMap(),
                    ),
                )
                .combineScript(
                    Script(
                        ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                        "long valueCount = 0; for (vc in state.valueCounts) { valueCount += vc } return valueCount", emptyMap(),
                    ),
                )
                .reduceScript(
                    Script(
                        ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                        "long valueCount = 0; for (vc in states) { valueCount += vc } return valueCount", emptyMap(),
                    ),
                )
        }

        is CardinalityAggregationBuilder -> {
            // Rewrite cardinality aggregation to use the .hll field
            // Customer queries: cardinality(field="user_id")
            // Rewritten query: cardinality(field="user_id.hll")

            // Log if user specified precisionThreshold in query (it will be ignored)
            if (aggregationBuilder.precisionThreshold() != null && aggregationBuilder.precisionThreshold() > 0) {
                logger.debug(
                    "Ignoring precisionThreshold parameter [{}] in cardinality query for field [{}]. " +
                        "Rollup indices use pre-computed HLL sketches with precision determined at rollup time. " +
                        "The query will use the precision from the rollup index mapping.",
                    aggregationBuilder.precisionThreshold(),
                    aggregationBuilder.field(),
                )
            }

            CardinalityAggregationBuilder(aggregationBuilder.name)
                .field(this.findMatchingMetricField<Cardinality>(aggregationBuilder.field()))
        }

        // We do nothing otherwise, the validation logic should have already verified so not throwing an exception
        else -> aggregationBuilder
    }
}

@Suppress("CyclomaticComplexMethod", "LongMethod")
fun Rollup.rewriteQueryBuilder(
    queryBuilder: QueryBuilder,
    fieldNameMappingTypeMap: Map<String, String>,
    concreteIndexName: String = "",
): QueryBuilder = when (queryBuilder) {
    is TermQueryBuilder -> {
        val updatedFieldName = queryBuilder.fieldName() + "." + Dimension.Type.TERMS.type
        val updatedTermQueryBuilder = TermQueryBuilder(updatedFieldName, queryBuilder.value())
        updatedTermQueryBuilder.boost(queryBuilder.boost())
        updatedTermQueryBuilder.queryName(queryBuilder.queryName())
    }

    is TermsQueryBuilder -> {
        val updatedFieldName = queryBuilder.fieldName() + "." + Dimension.Type.TERMS.type
        val updatedTermsQueryBuilder = TermsQueryBuilder(updatedFieldName, queryBuilder.values())
        updatedTermsQueryBuilder.boost(queryBuilder.boost())
        updatedTermsQueryBuilder.queryName(queryBuilder.queryName())
    }

    is RangeQueryBuilder -> {
        val updatedFieldName = queryBuilder.fieldName() + "." + fieldNameMappingTypeMap.getValue(queryBuilder.fieldName())
        val updatedRangeQueryBuilder = RangeQueryBuilder(updatedFieldName)
        updatedRangeQueryBuilder.includeLower(queryBuilder.includeLower())
        updatedRangeQueryBuilder.includeUpper(queryBuilder.includeUpper())
        updatedRangeQueryBuilder.from(queryBuilder.from())
        updatedRangeQueryBuilder.to(queryBuilder.to())
        if (queryBuilder.timeZone() != null) updatedRangeQueryBuilder.timeZone(queryBuilder.timeZone())
        if (queryBuilder.format() != null) updatedRangeQueryBuilder.format(queryBuilder.format())
        if (queryBuilder.relation()?.relationName != null) updatedRangeQueryBuilder.relation(queryBuilder.relation().relationName)
        updatedRangeQueryBuilder.queryName(queryBuilder.queryName())
        updatedRangeQueryBuilder.boost(queryBuilder.boost())
    }

    is BoolQueryBuilder -> {
        val newBoolQueryBuilder = BoolQueryBuilder()
        queryBuilder.must()?.forEach {
            val newMustQueryBuilder = this.rewriteQueryBuilder(it, fieldNameMappingTypeMap, concreteIndexName)
            newBoolQueryBuilder.must(newMustQueryBuilder)
        }
        queryBuilder.mustNot()?.forEach {
            val newMustNotQueryBuilder = this.rewriteQueryBuilder(it, fieldNameMappingTypeMap, concreteIndexName)
            newBoolQueryBuilder.mustNot(newMustNotQueryBuilder)
        }
        queryBuilder.should()?.forEach {
            val newShouldQueryBuilder = this.rewriteQueryBuilder(it, fieldNameMappingTypeMap, concreteIndexName)
            newBoolQueryBuilder.should(newShouldQueryBuilder)
        }
        queryBuilder.filter()?.forEach {
            val newFilterQueryBuilder = this.rewriteQueryBuilder(it, fieldNameMappingTypeMap, concreteIndexName)
            newBoolQueryBuilder.filter(newFilterQueryBuilder)
        }
        newBoolQueryBuilder.minimumShouldMatch(queryBuilder.minimumShouldMatch())
        newBoolQueryBuilder.adjustPureNegative(queryBuilder.adjustPureNegative())
        newBoolQueryBuilder.queryName(queryBuilder.queryName())
        newBoolQueryBuilder.boost(queryBuilder.boost())
    }

    is BoostingQueryBuilder -> {
        val newPositiveQueryBuilder = this.rewriteQueryBuilder(queryBuilder.positiveQuery(), fieldNameMappingTypeMap, concreteIndexName)
        val newNegativeQueryBuilder = this.rewriteQueryBuilder(queryBuilder.negativeQuery(), fieldNameMappingTypeMap, concreteIndexName)
        val newBoostingQueryBuilder = BoostingQueryBuilder(newPositiveQueryBuilder, newNegativeQueryBuilder)
        if (queryBuilder.negativeBoost() >= 0) newBoostingQueryBuilder.negativeBoost(queryBuilder.negativeBoost())
        newBoostingQueryBuilder.queryName(queryBuilder.queryName())
        newBoostingQueryBuilder.boost(queryBuilder.boost())
    }

    is ConstantScoreQueryBuilder -> {
        val newInnerQueryBuilder = this.rewriteQueryBuilder(queryBuilder.innerQuery(), fieldNameMappingTypeMap, concreteIndexName)
        val newConstantScoreQueryBuilder = ConstantScoreQueryBuilder(newInnerQueryBuilder)
        newConstantScoreQueryBuilder.boost(queryBuilder.boost())
        newConstantScoreQueryBuilder.queryName(queryBuilder.queryName())
    }

    is DisMaxQueryBuilder -> {
        val newDisMaxQueryBuilder = DisMaxQueryBuilder()
        queryBuilder.innerQueries().forEach {
            newDisMaxQueryBuilder.add(this.rewriteQueryBuilder(it, fieldNameMappingTypeMap, concreteIndexName))
        }
        newDisMaxQueryBuilder.tieBreaker(queryBuilder.tieBreaker())
        newDisMaxQueryBuilder.queryName(queryBuilder.queryName())
        newDisMaxQueryBuilder.boost(queryBuilder.boost())
    }

    is MatchPhraseQueryBuilder -> {
        val newFieldName = queryBuilder.fieldName() + "." + Dimension.Type.TERMS.type
        val newMatchPhraseQueryBuilder = MatchPhraseQueryBuilder(newFieldName, queryBuilder.value())
        newMatchPhraseQueryBuilder.queryName(queryBuilder.queryName())
        newMatchPhraseQueryBuilder.boost(queryBuilder.boost())
    }

    is QueryStringQueryBuilder -> {
        QueryStringQueryUtil.rewriteQueryStringQuery(queryBuilder, concreteIndexName)
    }

    // We do nothing otherwise, the validation logic should have already verified so not throwing an exception
    else -> queryBuilder
}

fun Set<Rollup>.buildRollupQuery(fieldNameMappingTypeMap: Map<String, String>, oldQuery: QueryBuilder, targetIndexName: String = ""): QueryBuilder {
    val wrappedQueryBuilder = BoolQueryBuilder()
    wrappedQueryBuilder.must(this.first().rewriteQueryBuilder(oldQuery, fieldNameMappingTypeMap, targetIndexName))
    wrappedQueryBuilder.should(TermsQueryBuilder("rollup._id", this.map { it.id }))
    wrappedQueryBuilder.minimumShouldMatch(1)
    return wrappedQueryBuilder
}

fun Rollup.populateFieldMappings(): Set<RollupFieldMapping> {
    val fieldMappings = mutableSetOf<RollupFieldMapping>()
    this.dimensions.forEach {
        fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.sourceField, it.type.type))
    }
    this.metrics.forEach { rollupMetric ->
        rollupMetric.metrics.forEach { metric ->
            fieldMappings.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, rollupMetric.sourceField, metric.type.type))
        }
    }
    return fieldMappings
}

// TODO: Not a fan of this.. but I can't find a way to overwrite the aggregations on the shallow copy or original
//  so we need to instantiate a new one so we can add the rewritten aggregation builders
@Suppress("CyclomaticComplexMethod")
fun SearchSourceBuilder.rewriteSearchSourceBuilder(
    jobs: Set<Rollup>,
    fieldNameMappingTypeMap: Map<String, String>,
    concreteIndexName: String,
): SearchSourceBuilder {
    val ssb = SearchSourceBuilder()
    // can use first() here as all jobs in the set will have a superset of the query's terms
    this.aggregations()?.aggregatorFactories?.forEach { ssb.aggregation(jobs.first().rewriteAggregationBuilder(it)) }
    if (this.explain() != null) ssb.explain(this.explain())
    if (this.ext() != null) ssb.ext(this.ext())
    ssb.fetchSource(this.fetchSource())
    this.docValueFields()?.forEach { ssb.docValueField(it.field, it.format) }
    ssb.storedFields(this.storedFields())
    if (this.from() >= 0) ssb.from(this.from())
    ssb.highlighter(this.highlighter())
    this.indexBoosts()?.forEach { ssb.indexBoost(it.index, it.boost) }
    if (this.minScore() != null) ssb.minScore(this.minScore())
    if (this.postFilter() != null) ssb.postFilter(this.postFilter())
    ssb.profile(this.profile())
    if (this.query() != null) ssb.query(jobs.buildRollupQuery(fieldNameMappingTypeMap, this.query(), concreteIndexName))
    this.rescores()?.forEach { ssb.addRescorer(it) }
    this.scriptFields()?.forEach { ssb.scriptField(it.fieldName(), it.script(), it.ignoreFailure()) }
    if (this.searchAfter() != null) ssb.searchAfter(this.searchAfter())
    if (this.slice() != null) ssb.slice(this.slice())
    if (this.size() >= 0) ssb.size(this.size())
    this.sorts()?.forEach { ssb.sort(it) }
    if (this.stats() != null) ssb.stats(this.stats())
    if (this.suggest() != null) ssb.suggest(this.suggest())
    if (this.terminateAfter() >= 0) ssb.terminateAfter(this.terminateAfter())
    if (this.timeout() != null) ssb.timeout(this.timeout())
    ssb.trackScores(this.trackScores())
    this.trackTotalHitsUpTo()?.let { ssb.trackTotalHitsUpTo(it) }
    if (this.version() != null) ssb.version(this.version())
    if (this.seqNoAndPrimaryTerm() != null) ssb.seqNoAndPrimaryTerm(this.seqNoAndPrimaryTerm())
    if (this.collapse() != null) ssb.collapse(this.collapse())
    return ssb
}

fun SearchSourceBuilder.rewriteSearchSourceBuilder(
    job: Rollup,
    fieldNameMappingTypeMap: Map<String, String>,
    concreteIndexName: String,
): SearchSourceBuilder = this.rewriteSearchSourceBuilder(setOf(job), fieldNameMappingTypeMap, concreteIndexName)

fun Rollup.getInitialDocValues(docCount: Long): MutableMap<String, Any?> =
    mutableMapOf(
        Rollup.ROLLUP_DOC_ID_FIELD to this.id,
        Rollup.ROLLUP_DOC_COUNT_FIELD to docCount,
        Rollup.ROLLUP_DOC_SCHEMA_VERSION_FIELD to this.schemaVersion,
    )

fun parseRollup(response: GetResponse, xContentRegistry: NamedXContentRegistry = NamedXContentRegistry.EMPTY): Rollup {
    val xcp =
        XContentHelper.createParser(
            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
            response.sourceAsBytesRef, XContentType.JSON,
        )

    return xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, Rollup.Companion::parse)
}
