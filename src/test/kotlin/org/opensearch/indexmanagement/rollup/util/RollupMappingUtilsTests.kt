/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.util

import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Cardinality
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.indexmanagement.rollup.randomRollupMetrics
import org.opensearch.test.OpenSearchTestCase

class RollupMappingUtilsTests : OpenSearchTestCase() {

    fun `test hasCardinalityMetrics returns true when cardinality present`() {
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "user_id",
                metrics = listOf(Cardinality(precisionThreshold = 1000)),
            ),
        )
        val rollup = randomRollup().copy(metrics = metrics)

        assertTrue(RollupMappingUtils.hasCardinalityMetrics(rollup))
    }

    fun `test hasCardinalityMetrics returns false when no cardinality`() {
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "value",
                metrics = listOf(Sum(), Max(), Min(), Average()),
            ),
        )
        val rollup = randomRollup().copy(metrics = metrics)

        assertFalse(RollupMappingUtils.hasCardinalityMetrics(rollup))
    }

    fun `test hasCardinalityMetrics returns true with mixed metrics`() {
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "value",
                metrics = listOf(Sum(), Max(), Cardinality(precisionThreshold = 400)),
            ),
        )
        val rollup = randomRollup().copy(metrics = metrics)

        assertTrue(RollupMappingUtils.hasCardinalityMetrics(rollup))
    }

    fun `test hasCardinalityMetrics returns false with empty metrics`() {
        val rollup = randomRollup().copy(metrics = emptyList())

        assertFalse(RollupMappingUtils.hasCardinalityMetrics(rollup))
    }

    fun `test buildCardinalityFieldMappings creates correct JSON structure`() {
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "user_id",
                targetField = "user_id",
                metrics = listOf(Cardinality(precisionThreshold = 400)),
            ),
        )
        val rollup = randomRollup().copy(metrics = metrics)

        val mappingsJson = RollupMappingUtils.buildCardinalityFieldMappings(rollup)

        // Parse the JSON to verify structure
        val parser = createParser(XContentType.JSON.xContent(), mappingsJson)
        val mappingsMap = parser.map()

        // Verify structure
        assertNotNull(mappingsMap["properties"])
        val properties = mappingsMap["properties"] as Map<*, *>
        assertNotNull(properties["user_id"])

        val userIdProps = properties["user_id"] as Map<*, *>
        assertNotNull(userIdProps["properties"])

        val userIdSubProps = userIdProps["properties"] as Map<*, *>
        assertNotNull(userIdSubProps["hll"])

        val hllMapping = userIdSubProps["hll"] as Map<*, *>
        assertEquals("hll", hllMapping["type"])
        assertEquals(12, hllMapping["precision"]) // precision_threshold 400 -> precision 12
        assertEquals(true, hllMapping["doc_values"])
    }

    fun `test buildCardinalityFieldMappings with nested field`() {
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "metrics.user_id",
                targetField = "metrics.user_id",
                metrics = listOf(Cardinality(precisionThreshold = 1000)),
            ),
        )
        val rollup = randomRollup().copy(metrics = metrics)

        val mappingsJson = RollupMappingUtils.buildCardinalityFieldMappings(rollup)

        // Parse and verify nested structure
        val parser = createParser(XContentType.JSON.xContent(), mappingsJson)
        val mappingsMap = parser.map()

        val properties = mappingsMap["properties"] as Map<*, *>
        assertNotNull(properties["metrics"])

        val metricsProps = properties["metrics"] as Map<*, *>
        assertNotNull(metricsProps["properties"])

        val metricsSubProps = metricsProps["properties"] as Map<*, *>
        assertNotNull(metricsSubProps["user_id"])

        val userIdProps = metricsSubProps["user_id"] as Map<*, *>
        assertNotNull(userIdProps["properties"])

        val userIdSubProps = userIdProps["properties"] as Map<*, *>
        assertNotNull(userIdSubProps["hll"])

        val hllMapping = userIdSubProps["hll"] as Map<*, *>
        assertEquals("hll", hllMapping["type"])
        assertEquals(13, hllMapping["precision"]) // precision_threshold 1000 -> precision 13
        assertEquals(true, hllMapping["doc_values"])
    }

    fun `test buildCardinalityFieldMappings with multiple cardinality metrics`() {
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "user_id",
                targetField = "user_id",
                metrics = listOf(Cardinality(precisionThreshold = 400)),
            ),
            randomRollupMetrics().copy(
                sourceField = "session_id",
                targetField = "session_id",
                metrics = listOf(Cardinality(precisionThreshold = 1000)),
            ),
        )
        val rollup = randomRollup().copy(metrics = metrics)

        val mappingsJson = RollupMappingUtils.buildCardinalityFieldMappings(rollup)

        // Parse and verify both fields exist
        val parser = createParser(XContentType.JSON.xContent(), mappingsJson)
        val mappingsMap = parser.map()

        val properties = mappingsMap["properties"] as Map<*, *>

        // Verify user_id field
        assertNotNull(properties["user_id"])
        val userIdProps = properties["user_id"] as Map<*, *>
        val userIdSubProps = (userIdProps["properties"] as Map<*, *>)["hll"] as Map<*, *>
        assertEquals("hll", userIdSubProps["type"])
        assertEquals(12, userIdSubProps["precision"])

        // Verify session_id field
        assertNotNull(properties["session_id"])
        val sessionIdProps = properties["session_id"] as Map<*, *>
        val sessionIdSubProps = (sessionIdProps["properties"] as Map<*, *>)["hll"] as Map<*, *>
        assertEquals("hll", sessionIdSubProps["type"])
        assertEquals(13, sessionIdSubProps["precision"])
    }

    fun `test buildCardinalityFieldMappings with mixed metrics only includes cardinality`() {
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "value",
                targetField = "value",
                metrics = listOf(
                    Sum(),
                    Max(),
                    Cardinality(precisionThreshold = 400),
                    Average(),
                ),
            ),
        )
        val rollup = randomRollup().copy(metrics = metrics)

        val mappingsJson = RollupMappingUtils.buildCardinalityFieldMappings(rollup)

        // Parse and verify only HLL field is present
        val parser = createParser(XContentType.JSON.xContent(), mappingsJson)
        val mappingsMap = parser.map()

        val properties = mappingsMap["properties"] as Map<*, *>
        assertNotNull(properties["value"])

        val valueProps = properties["value"] as Map<*, *>
        val valueSubProps = valueProps["properties"] as Map<*, *>

        // Should only have hll field, not sum, max, avg
        assertEquals(1, valueSubProps.size)
        assertNotNull(valueSubProps["hll"])
        assertNull(valueSubProps["sum"])
        assertNull(valueSubProps["max"])
        assertNull(valueSubProps["avg"])
    }

    fun `test buildCardinalityFieldMappings precision calculation`() {
        // Test various precision_threshold values and their corresponding precision
        val testCases = listOf(
            100L to 10, // precision_threshold 100 -> precision 10
            400L to 12, // precision_threshold 400 -> precision 12
            1000L to 13, // precision_threshold 1000 -> precision 13
            3000L to 14, // precision_threshold 3000 -> precision 14
            10000L to 16, // precision_threshold 10000 -> precision 16
        )

        testCases.forEach { (precisionThreshold, expectedPrecision) ->
            val metrics = listOf(
                randomRollupMetrics().copy(
                    sourceField = "test_field",
                    targetField = "test_field",
                    metrics = listOf(Cardinality(precisionThreshold = precisionThreshold)),
                ),
            )
            val rollup = randomRollup().copy(metrics = metrics)

            val mappingsJson = RollupMappingUtils.buildCardinalityFieldMappings(rollup)
            val parser = createParser(XContentType.JSON.xContent(), mappingsJson)
            val mappingsMap = parser.map()

            val properties = mappingsMap["properties"] as Map<*, *>
            val testFieldProps = properties["test_field"] as Map<*, *>
            val hllMapping = (testFieldProps["properties"] as Map<*, *>)["hll"] as Map<*, *>

            assertEquals(
                "precision_threshold $precisionThreshold should map to precision $expectedPrecision",
                expectedPrecision,
                hllMapping["precision"],
            )
        }
    }
}
