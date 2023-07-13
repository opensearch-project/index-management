/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.opensearch.indexmanagement.rollup.model.Rollup.Companion.DIMENSIONS_FIELD
import org.opensearch.indexmanagement.rollup.model.Rollup.Companion.METRICS_FIELD

/**
 * This holds the metadata of supported rollup operations
 * This is used to check if the rollup query is valid
 *
 * @param fieldName: the field to be operated on
 * @param mappingType: the mapping of the field
 */
data class RollupFieldMapping(val fieldType: FieldType, val fieldName: String, val mappingType: String, var sourceType: String? = null) {

    fun sourceType(type: String?) {
        this.sourceType = type
    }

    override fun toString(): String {
        return "$fieldName.$mappingType"
    }

    fun toIssue(isFieldMissing: Boolean = false): String {
        return if (isFieldMissing || mappingType == UNKNOWN_MAPPING)
            return "missing field $fieldName"
        else when (fieldType) {
            FieldType.METRIC -> "missing $mappingType aggregation on $fieldName"
            else -> "missing $mappingType grouping on $fieldName"
        }
    }

    companion object {
        // TODO explain what is this used for
        // range query is unknown mapping
        const val UNKNOWN_MAPPING = "unknown"

        enum class FieldType(val type: String) {
            DIMENSION(DIMENSIONS_FIELD),
            METRIC(METRICS_FIELD);

            override fun toString(): String {
                return type
            }
        }
    }
}
