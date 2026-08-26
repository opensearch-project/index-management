/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import org.junit.Before
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.Companion.BYPASS_ROLLUP_SEARCH
import org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.Companion.BYPASS_SIZE_CHECK
import org.opensearch.indexmanagement.rollup.model.RollupFieldMapping
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.indexmanagement.rollup.settings.RollupSettings
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.search.internal.ShardSearchRequest
import org.opensearch.test.OpenSearchTestCase
import java.time.ZoneId
import kotlin.test.assertFailsWith

class RollupInterceptorTests : OpenSearchTestCase() {

    private lateinit var interceptor: RollupInterceptor

    @Before
    fun setup() {
        interceptor = createInterceptor()
    }

    fun `test getBypassFromFetchSource returns null when no source`() {
        val request = mock(ShardSearchRequest::class.java)
        `when`(request.source()).thenReturn(null)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertNull(bypassLevel)
    }

    fun `test getBypassFromFetchSource returns null when no FetchSourceContext`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertNull(bypassLevel)
    }

    fun `test getBypassFromFetchSource returns null when no includes array`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(FetchSourceContext.FETCH_SOURCE)

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertNull(bypassLevel)
    }

    fun `test getBypassFromFetchSource returns null when no bypass marker present`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(FetchSourceContext(false, arrayOf("field1", "field2"), emptyArray()))

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertNull(bypassLevel)
    }

    fun `test getBypassFromFetchSource extracts BYPASS_ROLLUP_SEARCH correctly`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(FetchSourceContext(false, arrayOf("_rollup_internal_bypass_$BYPASS_ROLLUP_SEARCH"), emptyArray()))

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertEquals(BYPASS_ROLLUP_SEARCH, bypassLevel)
    }

    fun `test getBypassFromFetchSource extracts BYPASS_SIZE_CHECK correctly`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(FetchSourceContext(false, arrayOf("_rollup_internal_bypass_$BYPASS_SIZE_CHECK"), emptyArray()))

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertEquals(BYPASS_SIZE_CHECK, bypassLevel)
    }

    fun `test getBypassFromFetchSource finds marker among multiple includes`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(
            FetchSourceContext(false, arrayOf("field1", "_rollup_internal_bypass_$BYPASS_ROLLUP_SEARCH", "field2"), emptyArray()),
        )

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertEquals(BYPASS_ROLLUP_SEARCH, bypassLevel)
    }

    fun `test getBypassFromFetchSource returns null for invalid bypass marker`() {
        val request = mock(ShardSearchRequest::class.java)
        val source = SearchSourceBuilder()
        source.fetchSource(FetchSourceContext(false, arrayOf("_rollup_internal_bypass_invalid"), emptyArray()))

        `when`(request.source()).thenReturn(source)

        val bypassLevel = interceptor.getBypassFromFetchSource(request)

        assertNull(bypassLevel)
    }

    fun `test findMatchingRollupJobs returns match when rollup job covers all queried fields`() {
        val rollup = randomRollup().copy(
            dimensions = listOf(
                DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                Terms("field1", "field1"),
                Terms("field2", "field2"),
            ),
        )
        val fieldMappings = setOf(
            RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, "field1", Dimension.Type.TERMS.type),
        )

        val (matchingJobs, issues) = interceptor.findMatchingRollupJobs(fieldMappings, listOf(rollup))

        assertTrue("Expected matching jobs but got none", matchingJobs.isNotEmpty())
        assertTrue("Expected no issues but got $issues", issues.isEmpty())
    }

    fun `test findMatchingRollupJobs returns no match when rollup job missing queried field`() {
        val rollup = randomRollup().copy(
            dimensions = listOf(
                DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                Terms("field1", "field1"),
            ),
        )
        val fieldMappings = setOf(
            RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, "field2", Dimension.Type.TERMS.type),
        )

        val (matchingJobs, issues) = interceptor.findMatchingRollupJobs(fieldMappings, listOf(rollup))

        assertTrue("Expected no matching jobs", matchingJobs.isEmpty())
        assertTrue("Expected issues to be reported", issues.isNotEmpty())
    }

    fun `test findMatchingRollupJobs with multiple jobs only matches superset job`() {
        val rollupWithField1Only = randomRollup().copy(
            id = "rollup_1",
            dimensions = listOf(
                DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                Terms("field1", "field1"),
            ),
        )
        val rollupWithBothFields = randomRollup().copy(
            id = "rollup_2",
            dimensions = listOf(
                DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                Terms("field1", "field1"),
                Terms("field2", "field2"),
            ),
        )
        val fieldMappings = setOf(
            RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, "field2", Dimension.Type.TERMS.type),
        )

        val (matchingJobs, issues) = interceptor.findMatchingRollupJobs(
            fieldMappings,
            listOf(rollupWithField1Only, rollupWithBothFields),
        )

        assertEquals("Only the job with field2 should match", 1, matchingJobs.size)
        assertTrue("rollup_2 should be in matching jobs", matchingJobs.keys.any { it.id == "rollup_2" })
        assertTrue("Expected no issues", issues.isEmpty())
    }

    fun `test neutralizeAggregation rewrites supported aggregations to a nonexistent field`() {
        val interceptor = createInterceptor()
        val builders =
            listOf(
                TermsAggregationBuilder("agg_terms").field("category"),
                DateHistogramAggregationBuilder("agg_date_histogram").field("timestamp"),
                HistogramAggregationBuilder("agg_histogram").field("value").interval(10.0),
                SumAggregationBuilder("agg_sum").field("value"),
                AvgAggregationBuilder("agg_avg").field("value"),
                MaxAggregationBuilder("agg_max").field("value"),
                MinAggregationBuilder("agg_min").field("value"),
                ValueCountAggregationBuilder("agg_value_count").field("value"),
                CardinalityAggregationBuilder("agg_cardinality").field("value"),
            )

        for (builder in builders) {
            val result = interceptor.neutralizeAggregation(builder)
            assertEquals("Aggregation name should be preserved", builder.name, result.name)
            assertEquals("Aggregation type should be preserved", builder.type, result.type)
            assertEquals(
                "Aggregation should be rewritten to the nonexistent field",
                RollupInterceptor.NONEXISTENT_ROLLUP_FIELD,
                (result as ValuesSourceAggregationBuilder<*>).field(),
            )
        }
    }

    fun `test neutralizeAggregation preserves nested sub-aggregations`() {
        val interceptor = createInterceptor()
        val builder =
            DateHistogramAggregationBuilder("by_time").field("timestamp")
                .subAggregation(
                    TermsAggregationBuilder("by_vendor").field("vendor")
                        .subAggregation(SumAggregationBuilder("total").field("amount")),
                )

        val result = interceptor.neutralizeAggregation(builder)

        assertEquals("by_time", result.name)
        assertEquals(RollupInterceptor.NONEXISTENT_ROLLUP_FIELD, (result as ValuesSourceAggregationBuilder<*>).field())

        val vendorAgg = result.subAggregations.single()
        assertEquals("by_vendor", vendorAgg.name)
        assertEquals(RollupInterceptor.NONEXISTENT_ROLLUP_FIELD, (vendorAgg as ValuesSourceAggregationBuilder<*>).field())

        val totalAgg = vendorAgg.subAggregations.single()
        assertEquals("total", totalAgg.name)
        assertEquals("sum", totalAgg.type)
        assertEquals(RollupInterceptor.NONEXISTENT_ROLLUP_FIELD, (totalAgg as ValuesSourceAggregationBuilder<*>).field())
    }

    fun `test neutralizeAggregation throws on an unsupported aggregation type`() {
        val interceptor = createInterceptor()
        assertFailsWith(IllegalArgumentException::class) {
            interceptor.neutralizeAggregation(RangeAggregationBuilder("agg_range").field("value"))
        }
    }

    fun `test neutralizeAggregation preserves histogram intervals`() {
        val interceptor = createInterceptor()

        val fixedDh = DateHistogramAggregationBuilder("dh_fixed").field("ts").fixedInterval(DateHistogramInterval("30m"))
        val nFixed = interceptor.neutralizeAggregation(fixedDh) as DateHistogramAggregationBuilder
        assertEquals("fixed_interval should be preserved", DateHistogramInterval("30m"), nFixed.fixedInterval)
        assertEquals(RollupInterceptor.NONEXISTENT_ROLLUP_FIELD, nFixed.field())

        val calDh = DateHistogramAggregationBuilder("dh_cal").field("ts").calendarInterval(DateHistogramInterval("1d"))
        val nCal = interceptor.neutralizeAggregation(calDh) as DateHistogramAggregationBuilder
        assertEquals("calendar_interval should be preserved", DateHistogramInterval("1d"), nCal.calendarInterval)

        val hist = HistogramAggregationBuilder("hist").field("v").interval(50.0)
        val nHist = interceptor.neutralizeAggregation(hist) as HistogramAggregationBuilder
        assertEquals("histogram interval should be preserved", 50.0, nHist.interval(), 0.0)
    }

    fun `test neutralizeAggregation preserves date_histogram bucketing settings`() {
        val interceptor = createInterceptor()

        // time_zone, offset and min_doc_count must be preserved: dropping them makes the neutralized shard round
        // bucket boundaries differently from the raw shards, which corrupts the reduce (and fatally trips
        // InternalDateHistogram.addEmptyBuckets when assertions are enabled).
        val original =
            DateHistogramAggregationBuilder("by_day").field("ts")
                .calendarInterval(DateHistogramInterval("1d"))
                .timeZone(ZoneId.of("+05:30"))
                .offset(3600000L)
                .minDocCount(0)

        val neutralized = interceptor.neutralizeAggregation(original) as DateHistogramAggregationBuilder

        assertEquals(RollupInterceptor.NONEXISTENT_ROLLUP_FIELD, neutralized.field())
        assertEquals("calendar_interval should be preserved", DateHistogramInterval("1d"), neutralized.calendarInterval)
        assertEquals("time_zone should be preserved", ZoneId.of("+05:30"), neutralized.timeZone())
        assertEquals("offset should be preserved", 3600000L, neutralized.offset())
        assertEquals("min_doc_count should be preserved", 0L, neutralized.minDocCount())
    }

    // Helper method to create interceptor instance
    private fun createInterceptor(): RollupInterceptor {
        val clusterService = mock(ClusterService::class.java)
        val clusterSettings = ClusterSettings(
            Settings.EMPTY,
            setOf(
                RollupSettings.ROLLUP_SEARCH_ENABLED,
                RollupSettings.ROLLUP_SEARCH_ALL_JOBS,
                RollupSettings.ROLLUP_SEARCH_SOURCE_INDICES,
            ),
        )
        `when`(clusterService.clusterSettings).thenReturn(clusterSettings)

        val settings = Settings.EMPTY
        val resolver = mock(IndexNameExpressionResolver::class.java)
        return RollupInterceptor(clusterService, settings, resolver)
    }
}
