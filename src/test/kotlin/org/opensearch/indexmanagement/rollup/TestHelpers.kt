/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup

import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.common.model.dimension.Histogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.randomSchedule
import org.opensearch.indexmanagement.randomUser
import org.opensearch.indexmanagement.rollup.actionfilter.ISMFieldCapabilities
import org.opensearch.indexmanagement.rollup.actionfilter.ISMFieldCapabilitiesIndexResponse
import org.opensearch.indexmanagement.rollup.actionfilter.ISMFieldCapabilitiesResponse
import org.opensearch.indexmanagement.rollup.actionfilter.ISMIndexFieldCapabilities
import org.opensearch.indexmanagement.rollup.model.ContinuousMetadata
import org.opensearch.indexmanagement.rollup.model.ExplainRollup
import org.opensearch.indexmanagement.rollup.model.ISMRollup
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.RollupMetrics
import org.opensearch.indexmanagement.rollup.model.RollupStats
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Metric
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.model.metric.ValueCount
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.util.Locale

fun randomInterval(): String = if (OpenSearchRestTestCase.randomBoolean()) randomFixedInterval() else randomCalendarInterval()

@Suppress("FunctionOnlyReturningConstant")
fun randomCalendarInterval(): String = "1d"

@Suppress("FunctionOnlyReturningConstant")
fun randomFixedInterval(): String = "30m"

fun randomFixedDateHistogram(): DateHistogram = OpenSearchRestTestCase.randomAlphaOfLength(10).let {
    DateHistogram(sourceField = it, targetField = it, fixedInterval = randomFixedInterval(), calendarInterval = null, timezone = OpenSearchRestTestCase.randomZone())
}
fun randomCalendarDateHistogram(): DateHistogram = OpenSearchRestTestCase.randomAlphaOfLength(10).let {
    DateHistogram(sourceField = it, targetField = it, fixedInterval = null, calendarInterval = randomCalendarInterval(), timezone = OpenSearchRestTestCase.randomZone())
}

fun randomDateHistogram(): DateHistogram = if (OpenSearchRestTestCase.randomBoolean()) randomFixedDateHistogram() else randomCalendarDateHistogram()

fun randomHistogram(): Histogram = OpenSearchRestTestCase.randomAlphaOfLength(10).let {
    Histogram(
        sourceField = it,
        targetField = it,
        interval = OpenSearchRestTestCase.randomDoubleBetween(0.0, Double.MAX_VALUE, false) // start, end, lowerInclusive
    )
}

fun randomTerms(): Terms = OpenSearchRestTestCase.randomAlphaOfLength(10).let { Terms(sourceField = it, targetField = it) }

fun randomAverage(): Average = Average()

fun randomMax(): Max = Max()

fun randomMin(): Min = Min()

fun randomSum(): Sum = Sum()

fun randomValueCount(): ValueCount = ValueCount()

val metrics = listOf(randomAverage(), randomMax(), randomMin(), randomSum(), randomValueCount())

fun randomMetric(): Metric =
    OpenSearchRestTestCase.randomSubsetOf(1, metrics).first()

fun randomMetrics(): List<Metric> =
    OpenSearchRestTestCase.randomList(1, metrics.size, ::randomMetric).distinctBy { it.type }

fun randomRollupMetrics(): RollupMetrics = OpenSearchRestTestCase.randomAlphaOfLength(10).let {
    RollupMetrics(sourceField = it, targetField = it, metrics = randomMetrics())
}

fun randomRollupDimensions(): List<Dimension> {
    val dimensions = mutableListOf<Dimension>(randomDateHistogram())
    repeat(OpenSearchRestTestCase.randomInt(10) + 1) {
        dimensions.add(if (OpenSearchRestTestCase.randomBoolean()) randomTerms() else randomHistogram())
    }
    return dimensions.toList()
}

fun randomRollup(): Rollup {
    val enabled = OpenSearchRestTestCase.randomBoolean()
    return Rollup(
        id = OpenSearchRestTestCase.randomAlphaOfLength(10),
        seqNo = OpenSearchRestTestCase.randomNonNegativeLong(),
        primaryTerm = OpenSearchRestTestCase.randomNonNegativeLong(),
        enabled = enabled,
        schemaVersion = OpenSearchRestTestCase.randomLongBetween(1, 1000),
        jobSchedule = randomSchedule(),
        jobLastUpdatedTime = randomInstant(),
        jobEnabledTime = if (enabled) randomInstant() else null,
        description = OpenSearchRestTestCase.randomAlphaOfLength(10),
        sourceIndex = OpenSearchRestTestCase.randomAlphaOfLength(10).lowercase(Locale.ROOT),
        targetIndex = OpenSearchRestTestCase.randomAlphaOfLength(10).lowercase(Locale.ROOT),
        metadataID = if (OpenSearchRestTestCase.randomBoolean()) null else OpenSearchRestTestCase.randomAlphaOfLength(10),
        roles = OpenSearchRestTestCase.randomList(10) { OpenSearchRestTestCase.randomAlphaOfLength(10) },
        pageSize = OpenSearchRestTestCase.randomIntBetween(1, 10000),
        delay = 0,
        continuous = OpenSearchRestTestCase.randomBoolean(),
        dimensions = randomRollupDimensions(),
        metrics = OpenSearchRestTestCase.randomList(20, ::randomRollupMetrics).distinctBy { it.targetField },
        user = randomUser()
    )
}

fun randomRollupStats(): RollupStats {
    return RollupStats(
        pagesProcessed = OpenSearchRestTestCase.randomNonNegativeLong(),
        documentsProcessed = OpenSearchRestTestCase.randomNonNegativeLong(),
        rollupsIndexed = OpenSearchRestTestCase.randomNonNegativeLong(),
        indexTimeInMillis = OpenSearchRestTestCase.randomNonNegativeLong(),
        searchTimeInMillis = OpenSearchRestTestCase.randomNonNegativeLong()
    )
}

fun randomRollupMetadataStatus(): RollupMetadata.Status {
    return OpenSearchRestTestCase.randomFrom(RollupMetadata.Status.values().toList())
}

fun randomContinuousMetadata(): ContinuousMetadata {
    val one = randomInstant()
    val two = randomInstant()
    return ContinuousMetadata(
        nextWindowEndTime = if (one.isAfter(two)) one else two,
        nextWindowStartTime = if (one.isAfter(two)) two else one
    )
}

fun randomAfterKey(): Map<String, Any>? {
    return if (OpenSearchRestTestCase.randomBoolean()) {
        null
    } else {
        mapOf("test" to 17)
    }
}

fun randomRollupMetadata(): RollupMetadata {
    val status = randomRollupMetadataStatus()
    return RollupMetadata(
        id = OpenSearchRestTestCase.randomAlphaOfLength(10),
        seqNo = OpenSearchRestTestCase.randomNonNegativeLong(),
        primaryTerm = OpenSearchRestTestCase.randomNonNegativeLong(),
        rollupID = OpenSearchRestTestCase.randomAlphaOfLength(10),
        afterKey = randomAfterKey(),
        lastUpdatedTime = randomInstant(),
        continuous = randomContinuousMetadata(),
        status = status,
        failureReason = if (status == RollupMetadata.Status.FAILED) OpenSearchRestTestCase.randomAlphaOfLength(10) else null,
        stats = randomRollupStats()
    )
}

fun randomExplainRollup(): ExplainRollup {
    val metadata = randomRollupMetadata()
    return ExplainRollup(metadataID = metadata.id, metadata = metadata)
}

fun randomISMRollup(): ISMRollup {
    return ISMRollup(
        description = OpenSearchRestTestCase.randomAlphaOfLength(10),
        targetIndex = OpenSearchRestTestCase.randomAlphaOfLength(10).lowercase(Locale.ROOT),
        pageSize = OpenSearchRestTestCase.randomIntBetween(1, 10000),
        dimensions = randomRollupDimensions(),
        metrics = OpenSearchRestTestCase.randomList(20, ::randomRollupMetrics).distinctBy { it.targetField }
    )
}

fun randomISMFieldCapabilities(): ISMFieldCapabilities {
    return ISMFieldCapabilities(
        name = OpenSearchRestTestCase.randomAlphaOfLength(10),
        type = OpenSearchRestTestCase.randomAlphaOfLength(10),
        isSearchable = OpenSearchRestTestCase.randomBoolean(),
        isAggregatable = OpenSearchRestTestCase.randomBoolean(),
        indices = OpenSearchRestTestCase.generateRandomStringArray(10, 10, true, true),
        nonSearchableIndices = OpenSearchRestTestCase.generateRandomStringArray(10, 10, true, true),
        nonAggregatableIndices = OpenSearchRestTestCase.generateRandomStringArray(10, 10, true, true),
        meta = mapOf(OpenSearchRestTestCase.randomAlphaOfLength(10) to setOf(OpenSearchRestTestCase.randomAlphaOfLength(10)))
    )
}

fun randomISMIndexFieldCapabilities(): ISMIndexFieldCapabilities {
    return ISMIndexFieldCapabilities(
        name = OpenSearchRestTestCase.randomAlphaOfLength(10),
        type = OpenSearchRestTestCase.randomAlphaOfLength(10),
        isSearchable = OpenSearchRestTestCase.randomBoolean(),
        isAggregatable = OpenSearchRestTestCase.randomBoolean(),
        meta = mapOf(OpenSearchRestTestCase.randomAlphaOfLength(10) to OpenSearchRestTestCase.randomAlphaOfLength(10))
    )
}

fun randomISMFieldCapabilitiesIndexResponse(): ISMFieldCapabilitiesIndexResponse {
    return ISMFieldCapabilitiesIndexResponse(
        indexName = OpenSearchRestTestCase.randomAlphaOfLength(10),
        responseMap = mapOf(OpenSearchRestTestCase.randomAlphaOfLength(10) to randomISMIndexFieldCapabilities()),
        canMatch = OpenSearchRestTestCase.randomBoolean()
    )
}

fun randomISMFieldCaps(): ISMFieldCapabilitiesResponse {
    return ISMFieldCapabilitiesResponse(
        indices = OpenSearchRestTestCase.generateRandomStringArray(10, 10, false),
        responseMap = mapOf(OpenSearchRestTestCase.randomAlphaOfLength(10) to mapOf(OpenSearchRestTestCase.randomAlphaOfLength(10) to randomISMFieldCapabilities())),
        indexResponses = OpenSearchRestTestCase.randomList(4, ::randomISMFieldCapabilitiesIndexResponse)
    )
}

fun randomDimension(): Dimension {
    val dimensions = listOf(randomTerms(), randomHistogram(), randomDateHistogram())
    return OpenSearchRestTestCase.randomSubsetOf(1, dimensions).first()
}

fun randomTermQuery(): TermQueryBuilder { return TermQueryBuilder(OpenSearchRestTestCase.randomAlphaOfLength(5), OpenSearchRestTestCase.randomAlphaOfLength(5)) }

fun DateHistogram.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Histogram.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Terms.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Average.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Max.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Min.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Sum.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun ValueCount.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun RollupMetrics.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Rollup.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params).string()

fun RollupMetadata.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params).string()

fun ISMRollup.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params).string()
