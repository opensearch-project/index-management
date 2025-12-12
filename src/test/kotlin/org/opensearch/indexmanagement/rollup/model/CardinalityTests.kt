/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.opensearch.indexmanagement.rollup.model.metric.Cardinality
import org.opensearch.test.OpenSearchTestCase

class CardinalityTests : OpenSearchTestCase() {
    fun `test cardinality with default precision threshold`() {
        val cardinality = Cardinality()
        assertEquals(Cardinality.DEFAULT_PRECISION_THRESHOLD, cardinality.precisionThreshold)
    }

    fun `test cardinality with custom precision threshold`() {
        val customThreshold = 5000L
        val cardinality = Cardinality(precisionThreshold = customThreshold)
        assertEquals(customThreshold, cardinality.precisionThreshold)
    }

    fun `test cardinality with zero precision threshold throws exception`() {
        assertThrows(IllegalArgumentException::class.java) {
            Cardinality(precisionThreshold = 0)
        }
    }

    fun `test cardinality with negative precision threshold throws exception`() {
        assertThrows(IllegalArgumentException::class.java) {
            Cardinality(precisionThreshold = -100)
        }
    }

    fun `test precisionFromThreshold with small count`() {
        val precision = Cardinality.precisionFromThreshold(100)
        assertTrue("Precision should be at least MIN_PRECISION", precision >= 4)
        assertTrue("Precision should be at most MAX_PRECISION", precision <= 18)
    }

    fun `test precisionFromThreshold with large count`() {
        val precision = Cardinality.precisionFromThreshold(100000)
        assertTrue("Precision should be at least MIN_PRECISION", precision >= 4)
        assertTrue("Precision should be at most MAX_PRECISION", precision <= 18)
    }

    fun `test precisionFromThreshold with default threshold`() {
        val precision = Cardinality.precisionFromThreshold(Cardinality.DEFAULT_PRECISION_THRESHOLD)
        assertTrue("Precision should be at least MIN_PRECISION", precision >= 4)
        assertTrue("Precision should be at most MAX_PRECISION", precision <= 18)
    }

    fun `test cardinality equals and hashCode`() {
        val cardinality1 = Cardinality(precisionThreshold = 3000)
        val cardinality2 = Cardinality(precisionThreshold = 3000)
        val cardinality3 = Cardinality(precisionThreshold = 5000)

        assertEquals(cardinality1, cardinality2)
        assertEquals(cardinality1.hashCode(), cardinality2.hashCode())
        assertNotEquals(cardinality1, cardinality3)
        assertNotEquals(cardinality1.hashCode(), cardinality3.hashCode())
    }

    fun `test cardinality toString`() {
        val cardinality = Cardinality(precisionThreshold = 3000)
        val stringRepresentation = cardinality.toString()
        assertTrue(stringRepresentation.contains("Cardinality"))
        assertTrue(stringRepresentation.contains("3000"))
    }
}
