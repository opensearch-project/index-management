/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
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
        val exception = assertThrows(IllegalArgumentException::class.java) {
            Cardinality(precisionThreshold = 0)
        }
        assertTrue(exception.message!!.contains("Precision threshold must be positive"))
        assertTrue(exception.message!!.contains("0"))
    }

    fun `test cardinality with negative precision threshold throws exception`() {
        val exception = assertThrows(IllegalArgumentException::class.java) {
            Cardinality(precisionThreshold = -100)
        }
        assertTrue(exception.message!!.contains("Precision threshold must be positive"))
        assertTrue(exception.message!!.contains("-100"))
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

    fun `test cardinality hashCode is based on precisionThreshold`() {
        val cardinality = Cardinality(precisionThreshold = 8000)
        val expectedHashCode = 8000L.hashCode()
        assertEquals(expectedHashCode, cardinality.hashCode())
    }

    fun `test cardinality toString contains precisionThreshold`() {
        val cardinality = Cardinality(precisionThreshold = 3000)
        val stringRepresentation = cardinality.toString()
        assertEquals("Cardinality(precisionThreshold=3000)", stringRepresentation)
    }

    fun `test cardinality toString with different values`() {
        val testValues = listOf(100L, 1000L, 5000L, 10000L)
        testValues.forEach { threshold ->
            val cardinality = Cardinality(precisionThreshold = threshold)
            val stringRepresentation = cardinality.toString()
            assertTrue(stringRepresentation.contains("Cardinality"))
            assertTrue(stringRepresentation.contains(threshold.toString()))
        }
    }

    fun `test cardinality writeTo and constructor from StreamInput`() {
        val originalCardinality = Cardinality(precisionThreshold = 7500)

        // Serialize
        val output = BytesStreamOutput()
        originalCardinality.writeTo(output)

        // Deserialize
        val input = StreamInput.wrap(output.bytes().toBytesRef().bytes)
        val deserializedCardinality = Cardinality(input)

        // Verify
        assertEquals(originalCardinality.precisionThreshold, deserializedCardinality.precisionThreshold)
        assertEquals(originalCardinality, deserializedCardinality)
    }

    fun `test cardinality serialization with default precision threshold`() {
        val originalCardinality = Cardinality()

        // Serialize
        val output = BytesStreamOutput()
        originalCardinality.writeTo(output)

        // Deserialize
        val input = StreamInput.wrap(output.bytes().toBytesRef().bytes)
        val deserializedCardinality = Cardinality(input)

        // Verify
        assertEquals(Cardinality.DEFAULT_PRECISION_THRESHOLD, deserializedCardinality.precisionThreshold)
        assertEquals(originalCardinality, deserializedCardinality)
    }

    fun `test cardinality serialization with various precision thresholds`() {
        val thresholds = listOf(100L, 400L, 1000L, 3000L, 5000L, 10000L)

        thresholds.forEach { threshold ->
            val originalCardinality = Cardinality(precisionThreshold = threshold)

            // Serialize
            val output = BytesStreamOutput()
            originalCardinality.writeTo(output)

            // Deserialize
            val input = StreamInput.wrap(output.bytes().toBytesRef().bytes)
            val deserializedCardinality = Cardinality(input)

            // Verify
            assertEquals(threshold, deserializedCardinality.precisionThreshold)
            assertEquals(originalCardinality, deserializedCardinality)
        }
    }

    fun `test cardinality parse with invalid field throws exception`() {
        val invalidJson = """
            {
                "cardinality": {
                    "invalid_field": 1000
                }
            }
        """.trimIndent()

        val parser = createParser(org.opensearch.common.xcontent.XContentType.JSON.xContent(), invalidJson)
        parser.nextToken() // START_OBJECT
        parser.nextToken() // FIELD_NAME: cardinality
        parser.nextToken() // START_OBJECT

        val exception = assertThrows(IllegalArgumentException::class.java) {
            Cardinality.parse(parser)
        }

        assertTrue(exception.message!!.contains("Invalid field"))
        assertTrue(exception.message!!.contains("invalid_field"))
        assertTrue(exception.message!!.contains("cardinality metric"))
    }

    fun `test cardinality parse with valid precision threshold`() {
        val json = """
            {
                "cardinality": {
                    "precision_threshold": 5000
                }
            }
        """.trimIndent()

        val parser = createParser(org.opensearch.common.xcontent.XContentType.JSON.xContent(), json)
        parser.nextToken() // START_OBJECT
        parser.nextToken() // FIELD_NAME: cardinality
        parser.nextToken() // START_OBJECT

        val cardinality = Cardinality.parse(parser)

        assertEquals(5000L, cardinality.precisionThreshold)
    }

    fun `test cardinality parse without precision threshold uses default`() {
        val json = """
            {
                "cardinality": {}
            }
        """.trimIndent()

        val parser = createParser(org.opensearch.common.xcontent.XContentType.JSON.xContent(), json)
        parser.nextToken() // START_OBJECT
        parser.nextToken() // FIELD_NAME: cardinality
        parser.nextToken() // START_OBJECT

        val cardinality = Cardinality.parse(parser)

        assertEquals(Cardinality.DEFAULT_PRECISION_THRESHOLD, cardinality.precisionThreshold)
    }
}
