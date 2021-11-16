/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Histogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.model.metric.ValueCount
import org.opensearch.indexmanagement.rollup.randomAverage
import org.opensearch.indexmanagement.rollup.randomContinuousMetadata
import org.opensearch.indexmanagement.rollup.randomDateHistogram
import org.opensearch.indexmanagement.rollup.randomExplainRollup
import org.opensearch.indexmanagement.rollup.randomHistogram
import org.opensearch.indexmanagement.rollup.randomISMRollup
import org.opensearch.indexmanagement.rollup.randomMax
import org.opensearch.indexmanagement.rollup.randomMin
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.indexmanagement.rollup.randomRollupMetadata
import org.opensearch.indexmanagement.rollup.randomRollupMetrics
import org.opensearch.indexmanagement.rollup.randomRollupStats
import org.opensearch.indexmanagement.rollup.randomSum
import org.opensearch.indexmanagement.rollup.randomTerms
import org.opensearch.indexmanagement.rollup.randomValueCount
import org.opensearch.test.OpenSearchTestCase

class WriteableTests : OpenSearchTestCase() {

    fun `test date histogram dimension as stream`() {
        val dateHistogram = randomDateHistogram()
        val out = BytesStreamOutput().also { dateHistogram.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedDateHistogram = DateHistogram(sin)
        assertEquals("Round tripping Date Histogram stream doesn't work", dateHistogram, streamedDateHistogram)
    }

    fun `test histogram dimension as stream`() {
        val histogram = randomHistogram()
        val out = BytesStreamOutput().also { histogram.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedHistogram = Histogram(sin)
        assertEquals("Round tripping Histogram stream doesn't work", histogram, streamedHistogram)
    }

    fun `test terms dimension as stream`() {
        val terms = randomTerms()
        val out = BytesStreamOutput().also { terms.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedTerms = Terms(sin)
        assertEquals("Round tripping Terms stream doesn't work", terms, streamedTerms)
    }

    fun `test average metric as stream`() {
        val avg = randomAverage()
        val out = BytesStreamOutput().also { avg.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedAvg = Average(sin)
        assertEquals("Round tripping Average stream doesn't work", avg, streamedAvg)
    }

    fun `test max metric as stream`() {
        val max = randomMax()
        val out = BytesStreamOutput().also { max.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedMax = Max(sin)
        assertEquals("Round tripping Max stream doesn't work", max, streamedMax)
    }

    fun `test min metric as stream`() {
        val min = randomMin()
        val out = BytesStreamOutput().also { min.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedMin = Min(sin)
        assertEquals("Round tripping Min stream doesn't work", min, streamedMin)
    }

    fun `test sum metric as stream`() {
        val sum = randomSum()
        val out = BytesStreamOutput().also { sum.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedSum = Sum(sin)
        assertEquals("Round tripping Sum stream doesn't work", sum, streamedSum)
    }

    fun `test value_count metric as stream`() {
        val valueCount = randomValueCount()
        val out = BytesStreamOutput().also { valueCount.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedValueCount = ValueCount(sin)
        assertEquals("Round tripping ValueCount stream doesn't work", valueCount, streamedValueCount)
    }

    fun `test rollup metrics as stream`() {
        val rollupMetrics = randomRollupMetrics()
        val out = BytesStreamOutput().also { rollupMetrics.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRollupMetrics = RollupMetrics(sin)
        assertEquals("Round tripping RollupMetrics stream doesn't work", rollupMetrics, streamedRollupMetrics)
    }

    fun `test rollup as stream`() {
        val rollup = randomRollup().copy(delay = randomLongBetween(0, 60000000))
        val out = BytesStreamOutput().also { rollup.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRollup = Rollup(sin)
        assertEquals("Round tripping Rollup stream doesn't work", rollup, streamedRollup)
    }

    fun `test explain rollup as stream`() {
        val explainRollup = randomExplainRollup()
        val out = BytesStreamOutput().also { explainRollup.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedExplainRollup = ExplainRollup(sin)
        assertEquals("Round tripping ExplainRollup stream doesn't work", explainRollup, streamedExplainRollup)
    }

    fun `test continuous metadata as stream`() {
        val continuousMetadata = randomContinuousMetadata()
        val out = BytesStreamOutput().also { continuousMetadata.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedContinuousMetadata = ContinuousMetadata(sin)
        assertEquals("Round tripping ContinuousMetadata stream doesn't work", continuousMetadata, streamedContinuousMetadata)
    }

    fun `test rollup stats as stream`() {
        val stats = randomRollupStats()
        val out = BytesStreamOutput().also { stats.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedStats = RollupStats(sin)
        assertEquals("Round tripping RollupStats stream doesn't work", stats, streamedStats)
    }

    fun `test rollup metadata as stream`() {
        val rollupMetadata = randomRollupMetadata()
        val out = BytesStreamOutput().also { rollupMetadata.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRollupMetadata = RollupMetadata(sin)
        assertEquals("Round tripping RollupMetadata stream doesn't work", rollupMetadata, streamedRollupMetadata)
    }

    fun `test ism rollup as stream`() {
        val ismRollup = randomISMRollup()
        val out = BytesStreamOutput().also { ismRollup.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedISMRollup = ISMRollup(sin)
        assertEquals("Round tripping ISMRollup stream doesn't work", ismRollup, streamedISMRollup)
    }
}
