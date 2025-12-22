/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.util

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.Metadata
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
import org.opensearch.indexmanagement.rollup.randomDateHistogram
import org.opensearch.indexmanagement.rollup.randomHistogram
import org.opensearch.indexmanagement.rollup.randomMax
import org.opensearch.indexmanagement.rollup.randomMin
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.indexmanagement.rollup.randomSum
import org.opensearch.indexmanagement.rollup.randomTermQuery
import org.opensearch.indexmanagement.rollup.randomTerms
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
            val dimToAdd =
                when (it) {
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

    fun `test isRollupIndex`() {
        val indexName = "missing index"
        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()

        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(indexName)).doReturn(null)

        val actual = isRollupIndex(indexName, clusterState)
        assertFalse(actual)
    }

    fun `test getCompositeAggregationBuilder with non-rollup source index`() {
        val rollup = randomRollup()
        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.EMPTY

        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val compositeAgg = rollup.getCompositeAggregationBuilder(null, clusterState)

        assertEquals(rollup.pageSize, compositeAgg.size())
        assertEquals(rollup.dimensions.size, compositeAgg.sources().size)
    }

    fun `test getCompositeAggregationBuilder with rollup source index for dimensions`() {
        val dateHistogram = randomDateHistogram()
        val terms = randomTerms()
        val histogram = randomHistogram()
        val rollup = randomRollup().copy(dimensions = listOf(dateHistogram, terms, histogram))

        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.builder()
            .put(org.opensearch.indexmanagement.rollup.settings.RollupSettings.ROLLUP_INDEX.key, true).build()

        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val compositeAgg = rollup.getCompositeAggregationBuilder(null, clusterState)

        assertEquals(rollup.dimensions.size, compositeAgg.sources().size)
        compositeAgg.sources().forEachIndexed { index, source ->
            val dimension = rollup.dimensions[index]
            assertTrue(source.field().endsWith(".${dimension.type.type}"))
        }
    }

    fun `test getCompositeAggregationBuilder with rollup source index for Average metric`() {
        val rollupMetrics = RollupMetrics("test_field", "test_field", listOf(randomAverage()))
        val rollup = randomRollup().copy(metrics = listOf(rollupMetrics))

        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.builder()
            .put(org.opensearch.indexmanagement.rollup.settings.RollupSettings.ROLLUP_INDEX.key, true).build()

        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val compositeAgg = rollup.getCompositeAggregationBuilder(null, clusterState)

        val subAggs = compositeAgg.subAggregations
        assertEquals(2, subAggs.size)
        assertTrue(subAggs.any { it.name.endsWith(".avg.sum") })
        assertTrue(subAggs.any { it.name.endsWith(".avg.value_count") })
    }

    fun `test getCompositeAggregationBuilder with rollup source index for Sum metric`() {
        val rollupMetrics = RollupMetrics("test_field", "test_field", listOf(randomSum()))
        val rollup = randomRollup().copy(metrics = listOf(rollupMetrics))

        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.builder()
            .put(org.opensearch.indexmanagement.rollup.settings.RollupSettings.ROLLUP_INDEX.key, true).build()

        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val compositeAgg = rollup.getCompositeAggregationBuilder(null, clusterState)

        val subAggs = compositeAgg.subAggregations
        assertTrue(subAggs.any { it.name.endsWith(".sum") && it.type == "sum" })
    }

    fun `test getCompositeAggregationBuilder with rollup source index for Max metric`() {
        val rollupMetrics = RollupMetrics("test_field", "test_field", listOf(randomMax()))
        val rollup = randomRollup().copy(metrics = listOf(rollupMetrics))

        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.builder()
            .put(org.opensearch.indexmanagement.rollup.settings.RollupSettings.ROLLUP_INDEX.key, true).build()

        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val compositeAgg = rollup.getCompositeAggregationBuilder(null, clusterState)

        val subAggs = compositeAgg.subAggregations
        assertTrue(subAggs.any { it.name.endsWith(".max") && it.type == "max" })
    }

    fun `test getCompositeAggregationBuilder with rollup source index for Min metric`() {
        val rollupMetrics = RollupMetrics("test_field", "test_field", listOf(randomMin()))
        val rollup = randomRollup().copy(metrics = listOf(rollupMetrics))

        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.builder()
            .put(org.opensearch.indexmanagement.rollup.settings.RollupSettings.ROLLUP_INDEX.key, true).build()

        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val compositeAgg = rollup.getCompositeAggregationBuilder(null, clusterState)

        val subAggs = compositeAgg.subAggregations
        assertTrue(subAggs.any { it.name.endsWith(".min") && it.type == "min" })
    }

    fun `test getCompositeAggregationBuilder with rollup source index for ValueCount metric`() {
        val rollupMetrics = RollupMetrics("test_field", "test_field", listOf(randomValueCount()))
        val rollup = randomRollup().copy(metrics = listOf(rollupMetrics))

        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.builder()
            .put(org.opensearch.indexmanagement.rollup.settings.RollupSettings.ROLLUP_INDEX.key, true).build()

        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val compositeAgg = rollup.getCompositeAggregationBuilder(null, clusterState)

        val subAggs = compositeAgg.subAggregations
        assertTrue(subAggs.any { it.name.endsWith(".value_count") && it.type == "sum" })
    }

    fun `test getCompositeAggregationBuilder with rollup source index for multiple metrics`() {
        val rollupMetrics = RollupMetrics(
            "test_field", "test_field",
            listOf(randomAverage(), randomSum(), randomMax(), randomMin(), randomValueCount()),
        )
        val rollup = randomRollup().copy(metrics = listOf(rollupMetrics))

        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.builder()
            .put(org.opensearch.indexmanagement.rollup.settings.RollupSettings.ROLLUP_INDEX.key, true).build()

        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val compositeAgg = rollup.getCompositeAggregationBuilder(null, clusterState)

        val subAggs = compositeAgg.subAggregations
        assertEquals(6, subAggs.size)
    }

    fun `test getCompositeAggregationBuilder with afterKey`() {
        val rollup = randomRollup()
        val afterKey = mapOf("test_key" to "test_value")

        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.EMPTY

        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val compositeAgg = rollup.getCompositeAggregationBuilder(afterKey, clusterState)

        assertNotNull(compositeAgg)
    }

    fun `test getRollupSearchRequest with non-rollup source index`() {
        val rollup = randomRollup()
        val metadata = org.opensearch.indexmanagement.rollup.model.RollupMetadata(
            rollupID = rollup.id,
            lastUpdatedTime = java.time.Instant.now(),
            status = org.opensearch.indexmanagement.rollup.model.RollupMetadata.Status.STARTED,
            stats = org.opensearch.indexmanagement.rollup.model.RollupStats(0, 0, 0, 0, 0),
        )

        val clusterState: ClusterState = mock()
        val metadataObj: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.EMPTY

        whenever(clusterState.metadata).doReturn(metadataObj)
        whenever(metadataObj.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val searchRequest = rollup.getRollupSearchRequest(metadata, clusterState)

        assertNotNull(searchRequest)
        assertEquals(rollup.sourceIndex, searchRequest.indices()[0])
        val query = searchRequest.source().query()
        assertTrue(query is MatchAllQueryBuilder)
    }

    fun `test getRollupSearchRequest with rollup source index uses transformed date field`() {
        val dateHistogram = randomDateHistogram()
        val rollup = randomRollup().copy(dimensions = listOf(dateHistogram))
        val metadata = org.opensearch.indexmanagement.rollup.model.RollupMetadata(
            rollupID = rollup.id,
            lastUpdatedTime = java.time.Instant.now(),
            status = org.opensearch.indexmanagement.rollup.model.RollupMetadata.Status.STARTED,
            stats = org.opensearch.indexmanagement.rollup.model.RollupStats(0, 0, 0, 0, 0),
        )

        val clusterState: ClusterState = mock()
        val metadataObj: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.builder()
            .put(org.opensearch.indexmanagement.rollup.settings.RollupSettings.ROLLUP_INDEX.key, true).build()

        whenever(clusterState.metadata).doReturn(metadataObj)
        whenever(metadataObj.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val searchRequest = rollup.getRollupSearchRequest(metadata, clusterState)

        assertNotNull(searchRequest)
        val query = searchRequest.source().query()
        assertTrue(query is MatchAllQueryBuilder)
    }

    fun `test getRollupSearchRequest with continuous metadata and rollup source index`() {
        val dateHistogram = randomDateHistogram()
        val rollup = randomRollup().copy(dimensions = listOf(dateHistogram))
        val startTime = java.time.Instant.now().minusSeconds(3600)
        val endTime = java.time.Instant.now()
        val continuousMetadata = org.opensearch.indexmanagement.rollup.model.ContinuousMetadata(startTime, endTime)
        val metadata = org.opensearch.indexmanagement.rollup.model.RollupMetadata(
            rollupID = rollup.id,
            lastUpdatedTime = java.time.Instant.now(),
            continuous = continuousMetadata,
            status = org.opensearch.indexmanagement.rollup.model.RollupMetadata.Status.STARTED,
            stats = org.opensearch.indexmanagement.rollup.model.RollupStats(0, 0, 0, 0, 0),
        )

        val clusterState: ClusterState = mock()
        val metadataObj: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.builder()
            .put(org.opensearch.indexmanagement.rollup.settings.RollupSettings.ROLLUP_INDEX.key, true).build()

        whenever(clusterState.metadata).doReturn(metadataObj)
        whenever(metadataObj.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val searchRequest = rollup.getRollupSearchRequest(metadata, clusterState)

        assertNotNull(searchRequest)
        val query = searchRequest.source().query()
        assertTrue(query is RangeQueryBuilder)
        val rangeQuery = query as RangeQueryBuilder
        assertEquals("${dateHistogram.sourceField}.date_histogram", rangeQuery.fieldName())
        assertEquals(startTime.toEpochMilli(), rangeQuery.from())
        assertEquals(endTime.toEpochMilli(), rangeQuery.to())
    }

    fun `test getRollupSearchRequest with continuous metadata and non-rollup source index`() {
        val dateHistogram = randomDateHistogram()
        val rollup = randomRollup().copy(dimensions = listOf(dateHistogram))
        val startTime = java.time.Instant.now().minusSeconds(3600)
        val endTime = java.time.Instant.now()
        val continuousMetadata = org.opensearch.indexmanagement.rollup.model.ContinuousMetadata(startTime, endTime)
        val metadata = org.opensearch.indexmanagement.rollup.model.RollupMetadata(
            rollupID = rollup.id,
            lastUpdatedTime = java.time.Instant.now(),
            continuous = continuousMetadata,
            status = org.opensearch.indexmanagement.rollup.model.RollupMetadata.Status.STARTED,
            stats = org.opensearch.indexmanagement.rollup.model.RollupStats(0, 0, 0, 0, 0),
        )

        val clusterState: ClusterState = mock()
        val metadataObj: Metadata = mock()
        val indexMetadata: org.opensearch.cluster.metadata.IndexMetadata = mock()
        val settings = org.opensearch.common.settings.Settings.EMPTY

        whenever(clusterState.metadata).doReturn(metadataObj)
        whenever(metadataObj.index(rollup.sourceIndex)).doReturn(indexMetadata)
        whenever(indexMetadata.settings).doReturn(settings)

        val searchRequest = rollup.getRollupSearchRequest(metadata, clusterState)

        assertNotNull(searchRequest)
        val query = searchRequest.source().query()
        assertTrue(query is RangeQueryBuilder)
        val rangeQuery = query as RangeQueryBuilder
        assertEquals(dateHistogram.sourceField, rangeQuery.fieldName())
        assertEquals(startTime.toEpochMilli(), rangeQuery.from())
        assertEquals(endTime.toEpochMilli(), rangeQuery.to())
    }
}
