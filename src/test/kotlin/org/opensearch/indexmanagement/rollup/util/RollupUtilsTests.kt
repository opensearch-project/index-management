/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.util

import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.ConstantScoreQueryBuilder
import org.opensearch.index.query.DisMaxQueryBuilder
import org.opensearch.index.query.MatchAllQueryBuilder
import org.opensearch.index.query.MatchPhraseQueryBuilder
import org.opensearch.index.query.RangeQueryBuilder
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.index.query.TermsQueryBuilder
import org.opensearch.index.search.MatchQuery
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.common.model.dimension.Histogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.rollup.model.RollupFieldMapping
import org.opensearch.indexmanagement.rollup.model.RollupMetrics
import org.opensearch.indexmanagement.rollup.randomAverage
import org.opensearch.indexmanagement.rollup.randomMax
import org.opensearch.indexmanagement.rollup.randomMin
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.indexmanagement.rollup.randomSum
import org.opensearch.indexmanagement.rollup.randomTermQuery
import org.opensearch.indexmanagement.rollup.randomValueCount
import org.opensearch.indexmanagement.transform.randomAggregationBuilder
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.rest.OpenSearchRestTestCase

class RollupUtilsTests : OpenSearchTestCase() {

    fun `test rewriteQueryBuilder term query`() {
        val termQuery = randomTermQuery()
        termQuery.queryName("dummy-query")
        termQuery.boost(0.4f)
        val actual = randomRollup().rewriteQueryBuilder(termQuery, mapOf()) as TermQueryBuilder
        assertEquals(termQuery.boost(), actual.boost())
        assertEquals(termQuery.queryName(), actual.queryName())
        assertEquals(termQuery.fieldName() + ".terms", actual.fieldName())
        assertEquals(termQuery.value(), actual.value())
    }

    fun `test rewriteQueryBuilder terms query`() {
        val termsQuery = TermsQueryBuilder("some-field", "some-value")
        termsQuery.queryName("dummy-query")
        termsQuery.boost(0.4f)
        val actual = randomRollup().rewriteQueryBuilder(termsQuery, mapOf()) as TermsQueryBuilder
        assertEquals(termsQuery.boost(), actual.boost())
        assertEquals(termsQuery.queryName(), actual.queryName())
        assertEquals(termsQuery.fieldName() + ".terms", actual.fieldName())
        assertEquals(termsQuery.values(), actual.values())
    }

    fun `test rewriteQueryBuilder range query`() {
        val rangeQuery = RangeQueryBuilder("some-field")
        rangeQuery.includeUpper(false)
        rangeQuery.includeLower(true)
        rangeQuery.to("2020-10-10")
        rangeQuery.from("2020-10-01")
        rangeQuery.timeZone("UTC")
        rangeQuery.format("YYYY-MM-DD")
        rangeQuery.queryName("some-query")
        rangeQuery.boost(0.5f)
        val fieldMapping = mapOf("some-field" to "dummy-mapping")
        val actual = randomRollup().rewriteQueryBuilder(rangeQuery, fieldMapping) as RangeQueryBuilder
        assertEquals(rangeQuery.queryName(), actual.queryName())
        assertEquals(rangeQuery.boost(), actual.boost())
        assertEquals(rangeQuery.includeLower(), actual.includeLower())
        assertEquals(rangeQuery.includeUpper(), actual.includeUpper())
        assertEquals(rangeQuery.timeZone(), actual.timeZone())
        assertEquals(rangeQuery.format(), actual.format())
        assertEquals(rangeQuery.fieldName() + ".dummy-mapping", actual.fieldName())
    }

    fun `test rewriteQueryBuilder matchAll query`() {
        val matchAllQuery = MatchAllQueryBuilder()
        val actual = randomRollup().rewriteQueryBuilder(matchAllQuery, mapOf()) as MatchAllQueryBuilder
        assertEquals(matchAllQuery, actual)
    }

    fun `test rewriteQueryBuilder bool query`() {
        val boolQuery = BoolQueryBuilder()
        val mustQuery = randomTermQuery()
        val mustNotQuery = randomTermQuery()
        val shouldQuery = randomTermQuery()
        val filterQuery = randomTermQuery()
        boolQuery.minimumShouldMatch(1)
        boolQuery.adjustPureNegative(false)
        boolQuery.queryName("some-query")
        boolQuery.boost(1f)
        boolQuery.must(mustQuery)
        boolQuery.mustNot(mustNotQuery)
        boolQuery.should(shouldQuery)
        boolQuery.filter(filterQuery)

        val rollup = randomRollup()
        val actual = rollup.rewriteQueryBuilder(boolQuery, mapOf()) as BoolQueryBuilder
        assertEquals(boolQuery.queryName(), actual.queryName())
        assertEquals(boolQuery.boost(), actual.boost())
        assertEquals(boolQuery.adjustPureNegative(), actual.adjustPureNegative())
        assertEquals(boolQuery.minimumShouldMatch(), actual.minimumShouldMatch())
        assertEquals(boolQuery.filter().size, actual.filter().size)
        assertEquals(boolQuery.must().size, actual.must().size)
        assertEquals(boolQuery.mustNot().size, actual.mustNot().size)
        assertEquals(boolQuery.should().size, actual.should().size)
        assertEquals(rollup.rewriteQueryBuilder(filterQuery, mapOf()), actual.filter().first())
        assertEquals(rollup.rewriteQueryBuilder(mustQuery, mapOf()), actual.must().first())
        assertEquals(rollup.rewriteQueryBuilder(mustNotQuery, mapOf()), actual.mustNot().first())
        assertEquals(rollup.rewriteQueryBuilder(shouldQuery, mapOf()), actual.should().first())
    }

    fun `test rewriteQueryBuilder constant score query`() {
        val innerQuery = randomTermQuery()
        val constantQuery = ConstantScoreQueryBuilder(innerQuery)
        constantQuery.queryName("some-query")
        constantQuery.boost(0.2f)
        val rollup = randomRollup()
        val actual = rollup.rewriteQueryBuilder(constantQuery, mapOf()) as ConstantScoreQueryBuilder
        assertEquals(constantQuery.queryName(), actual.queryName())
        assertEquals(constantQuery.boost(), actual.boost())
        assertEquals(rollup.rewriteQueryBuilder(innerQuery, mapOf()), actual.innerQuery())
    }

    fun `test rewriteQueryBuilder disMax query`() {
        val innerQuery = randomTermQuery()
        val disMaxQuery = DisMaxQueryBuilder()
        disMaxQuery.add(innerQuery)
        disMaxQuery.tieBreaker(0.3f)
        disMaxQuery.queryName("some-query")
        disMaxQuery.boost(0.1f)
        val rollup = randomRollup()
        val actual = rollup.rewriteQueryBuilder(disMaxQuery, mapOf()) as DisMaxQueryBuilder
        assertEquals(disMaxQuery.queryName(), actual.queryName())
        assertEquals(disMaxQuery.boost(), actual.boost())
        assertEquals(disMaxQuery.tieBreaker(), actual.tieBreaker())
        assertEquals(disMaxQuery.innerQueries().size, actual.innerQueries().size)
        assertEquals(rollup.rewriteQueryBuilder(innerQuery, mapOf()), actual.innerQueries().first())
    }

    fun `test populateFieldMappings`() {
        val rollup = randomRollup()
        val expected = mutableSetOf<RollupFieldMapping>()
        rollup.dimensions.forEach {
            expected.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.sourceField, it.type.type))
        }
        rollup.metrics.forEach { rollupMetric ->
            rollupMetric.metrics.forEach { metric ->
                expected.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, rollupMetric.sourceField, metric.type.type))
            }
        }
        val actual = rollup.populateFieldMappings()
        assertEquals(expected, actual)
    }

    fun `test buildRollupQuery`() {
        val rollup = randomRollup()
        val queryBuilder = MatchAllQueryBuilder()
        val actual = setOf(rollup).buildRollupQuery(mapOf(), queryBuilder) as BoolQueryBuilder
        val expectedShould = TermsQueryBuilder("rollup._id", rollup.id)
        assertTrue(actual.mustNot().isEmpty())
        assertTrue(actual.filter().isEmpty())
        assertEquals("1", actual.minimumShouldMatch())
        assertEquals(1, actual.must().size)
        assertEquals(rollup.rewriteQueryBuilder(queryBuilder, mapOf()), actual.must().first())
        assertEquals(1, actual.should().size)
        assertEquals(1, (actual.should()[0] as TermsQueryBuilder).values().size)
        assertEquals(expectedShould, actual.should().first())
    }

    fun `test buildRollupQuery multiple`() {
        var rollups = setOf(randomRollup(), randomRollup(), randomRollup(), randomRollup(), randomRollup())
        rollups = OpenSearchRestTestCase.randomSubsetOf(randomIntBetween(2, 5), rollups).toSet()
        val queryBuilder = MatchAllQueryBuilder()
        val actual = rollups.buildRollupQuery(mapOf(), queryBuilder) as BoolQueryBuilder
        val expectedShould = TermsQueryBuilder("rollup._id", rollups.map { it.id })
        assertTrue(actual.mustNot().isEmpty())
        assertTrue(actual.filter().isEmpty())
        assertEquals("1", actual.minimumShouldMatch())
        assertEquals(1, actual.must().size)
        assertEquals(rollups.first().rewriteQueryBuilder(queryBuilder, mapOf()), actual.must().first())
        assertEquals(1, actual.should().size)
        assertEquals(rollups.size, (actual.should()[0] as TermsQueryBuilder).values().size)
        assertEquals(expectedShould, actual.should().first())
    }

    fun `test rewriteQueryBuilder match phrase query`() {
        val matchPhraseQuery = MatchPhraseQueryBuilder("dummy-field", "dummy-value")
        matchPhraseQuery.queryName("dummy-name")
        matchPhraseQuery.boost(0.1f)
        val rollup = randomRollup()
        val actual = rollup.rewriteQueryBuilder(matchPhraseQuery, mapOf()) as MatchPhraseQueryBuilder
        assertEquals(matchPhraseQuery.queryName(), actual.queryName())
        assertEquals(matchPhraseQuery.boost(), actual.boost())
        assertEquals(MatchQuery.DEFAULT_ZERO_TERMS_QUERY, actual.zeroTermsQuery())
        assertEquals(MatchQuery.DEFAULT_PHRASE_SLOP, actual.slop())
        assertEquals(matchPhraseQuery.fieldName() + ".terms", actual.fieldName())
        assertEquals(matchPhraseQuery.value(), actual.value())
        assertNull(actual.analyzer())
    }

    fun `test rewriteAggregationBuilder`() {
        var rollup = randomRollup()
        val aggBuilder = randomAggregationBuilder()
        val aggField = ((aggBuilder.convertToMap()[aggBuilder.name] as Map<*, *>).values.first() as Map<*, *>).values.first() as String
        val newDims = mutableListOf<Dimension>()
        // Make rollup dimensions and metrics contain the aggregation field name and aggregation metrics
        rollup.dimensions.forEach {
            val dimToAdd = when (it) {
                is DateHistogram -> it.copy(sourceField = aggField, targetField = aggField)
                is Terms -> it.copy(sourceField = aggField, targetField = aggField)
                is Histogram -> it.copy(sourceField = aggField, targetField = aggField)
                else -> it
            }
            newDims.add(dimToAdd)
        }
        val newMetrics = mutableListOf(RollupMetrics(aggField, aggField, listOf(randomAverage(), randomMax(), randomMin(), randomSum(), randomValueCount())))
        rollup = rollup.copy(dimensions = newDims, metrics = newMetrics)
        val rewrittenAgg = rollup.rewriteAggregationBuilder(aggBuilder)
        assertEquals("Rewritten aggregation builder does not have the same name", aggBuilder.name, rewrittenAgg.name)
        if (aggBuilder is AvgAggregationBuilder || aggBuilder is ValueCountAggregationBuilder) {
            assertEquals("Rewritten aggregation builder is not the correct type", "scripted_metric", rewrittenAgg.type)
        } else {
            assertEquals("Rewritten aggregation builder is not the correct type", aggBuilder.type, rewrittenAgg.type)
        }
    }
}
