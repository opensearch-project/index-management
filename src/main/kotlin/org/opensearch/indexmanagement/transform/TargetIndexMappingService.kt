/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.opensearch.client.Client
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.transform.exceptions.TransformIndexException
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.util.IndexUtils

/**
 * Service designed for creating dynamic target index mapping based on the date field types of the source index.
 * Creates target index date properties based on the date properties of the source index (ie. if the term grouping is applied on a date field of source index, target index field will have date type also)
 */
class TargetIndexMappingService(private val client: Client) {
    companion object {
        private const val TYPE = "type"
        private const val PROPERTIES = "properties"
        private const val METADATA = "_meta"
        private const val SCHEMA_VERSION = "schema_version"
        private const val DYNAMIC_TEMPLATE = "dynamic_templates"
        private const val MATCH_MAPPING_TYPE = "match_mapping_type"
        private const val MAPPING = "mapping"
    }

    suspend fun buildTargetIndexMapping(transform: Transform): String {
        val request = GetMappingsRequest().indices(transform.sourceIndex)

        val result: GetMappingsResponse = client.admin().indices().suspendUntil { getMappings(request, it) }
            ?: error("GetMappingResponse for [$transform.sourceIndex] was null")

        if (result.mappings[transform.sourceIndex] == null) {
            return IndexManagementIndices.transformTargetMappings
        }

        val sourceIndexMapping = result.mappings[transform.sourceIndex].sourceAsMap

        val dateCompositeAggregations = mutableMapOf<String, Any>()
        transform.groups.forEach { dimension ->
            if (!isFieldInMappings(dimension.sourceField, sourceIndexMapping)) {
                throw TransformIndexException("Missing field ${dimension.sourceField} in source index")
            }
            val sourceFieldType = IndexUtils.getFieldFromMappings(dimension.sourceField, sourceIndexMapping)
            // Consider only date fields as relevant for building the target index mapping
            if (sourceFieldType?.get(TYPE) != null && sourceFieldType[TYPE] == "date") {
                dateCompositeAggregations[dimension.targetField] = sourceFieldType[TYPE]!!
            }
        }

        return mapTargetIndex(dateCompositeAggregations)
    }

    private fun mapTargetIndex(dateCompositeAggregations: MutableMap<String, Any>): String {
        // Build static properties
        val builder = XContentFactory.jsonBuilder().startObject()
            .startObject(METADATA)
            .field(SCHEMA_VERSION, 1)
            .endObject()
            .startArray(DYNAMIC_TEMPLATE)
            .startObject()
            .startObject("strings")
            .field(MATCH_MAPPING_TYPE, "string")
            .startObject(MAPPING)
            .field(TYPE, "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .startObject(PROPERTIES)

        // Dynamically build composite aggregation mapping
        mapCompositeAggregation(dateCompositeAggregations, builder)

        // Close the object and return as a string
        return builder.endObject()
            .endObject()
            .string()
    }

    @Suppress("UNCHECKED_CAST")
    private fun mapCompositeAggregation(
        compositeAggregation: Map<String, Any>,
        builder: XContentBuilder,
    ) {
        val iterator = compositeAggregation.entries.iterator()
        while (iterator.hasNext()) {
            val it = iterator.next()
            if (it.value is Map<*, *>) {
                builder.startObject(it.key)
                // Start object until reaching the "leaf"; leaf is the last key value pair, where value is not a map
                mapCompositeAggregation(it.value as Map<String, Any>, builder)
                builder.endObject()
            } else {
                builder.field(it.key, it.value.toString())
            }
        }
    }

    private fun isFieldInMappings(fieldName: String, mappings: Map<*, *>): Boolean {
        val field = IndexUtils.getFieldFromMappings(fieldName, mappings)
        return field != null
    }
}
