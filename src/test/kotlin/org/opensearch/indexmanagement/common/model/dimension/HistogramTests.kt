/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.common.model.dimension

import org.junit.Assert
import org.opensearch.index.query.RangeQueryBuilder
import org.opensearch.indexmanagement.rollup.randomHistogram
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class HistogramTests : OpenSearchTestCase() {
    fun `test histogram to bucket query has correct values`() {
        val histogram = randomHistogram()
        val randomKey = randomDouble()
        val bucketQuery = histogram.toBucketQuery(randomKey) as RangeQueryBuilder

        assertEquals("Histogram bucket query did not contain the correct interval", histogram.interval, bucketQuery.to() as Double - bucketQuery.from() as Double, 0.001)
        assertEquals("Histogram bucket query did not contain the correct bucket start", randomKey, bucketQuery.from() as Double, 0.0001)
        assertEquals("Histogram bucket query did not contain the correct field name", bucketQuery.fieldName(), histogram.sourceField)
        Assert.assertTrue("Histogram bucket query should include the lower bounds", bucketQuery.includeLower())
        Assert.assertTrue("Histogram bucket query should include the upper bounds", bucketQuery.includeUpper())
    }

    fun `test histogram to bucket query fails with wrong bucket key type`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException with type Int when Double is expected") {
            randomHistogram().toBucketQuery(randomInt())
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException with type Long when Double is expected") {
            randomHistogram().toBucketQuery(randomLong())
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException with type Float when Double is expected") {
            randomHistogram().toBucketQuery(randomFloat())
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException with type String when Double is expected") {
            randomHistogram().toBucketQuery(randomAlphaOfLengthBetween(1, 10))
        }
    }
}
