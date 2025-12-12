/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.util

import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.metric.Cardinality

/**
 * Utility functions for building rollup index mappings based on rollup job configuration.
 */
object RollupMappingUtils {

    /**
     * Checks if the rollup job has any cardinality metrics.
     *
     * @param rollup The rollup job configuration
     * @return true if the rollup has cardinality metrics, false otherwise
     */
    fun hasCardinalityMetrics(rollup: Rollup): Boolean =
        rollup.metrics.any { rollupMetrics ->
            rollupMetrics.metrics.any { it is Cardinality }
        }

    /**
     * Builds explicit field mappings for cardinality metrics.
     * These mappings ensure sketch fields are created with HLL type before any documents are indexed.
     *
     * @param rollup The rollup job configuration
     * @return JSON string representing the properties mapping for cardinality fields
     */
    fun buildCardinalityFieldMappings(rollup: Rollup): String {
        val mappings = mutableListOf<String>()

        rollup.metrics.forEach { rollupMetrics ->
            rollupMetrics.metrics.forEach { metric ->
                if (metric is Cardinality) {
                    val targetField = rollupMetrics.targetField
                    mappings.add(buildFieldMapping(targetField, metric.precisionThreshold))
                }
            }
        }

        return """{"properties":{${mappings.joinToString(",")}}}"""
    }

    /**
     * Builds the JSON mapping for a single cardinality field.
     * Creates structure: targetField.hll with HLL type and precision.
     *
     * @param targetField The target field name (e.g., "value", "user_id")
     * @param precisionThreshold The precision threshold which will be converted to HLL precision
     * @return JSON string for the field mapping
     */
    private fun buildFieldMapping(targetField: String, precisionThreshold: Long): String {
        val fieldParts = targetField.split(".")

        // Convert precision threshold to actual precision
        val precision = Cardinality.precisionFromThreshold(precisionThreshold)

        // Build nested structure
        val opening = fieldParts.joinToString("") { """"$it":{"properties":{""" }
        val closing = "}}".repeat(fieldParts.size)

        // HLL field mapping with precision and doc_values enabled
        val hllMapping = """"hll":{"type":"hll","precision":$precision,"doc_values":true}"""

        return "$opening$hllMapping$closing"
    }
}
