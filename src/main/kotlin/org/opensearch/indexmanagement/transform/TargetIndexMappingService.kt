/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.opensearch.client.Client
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.transform.exceptions.TransformIndexException
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.util.DEFAULT_DATE_FORMAT
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.search.aggregations.AggregationBuilder
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder

/**
 * Service designed for creating dynamic target index mapping based on the date field types of the source index.
 * Creates target index date properties based on the date properties of the source index
 * (ie. if the term grouping is applied on a date field of source index, target index field will have date type also)
 */
object TargetIndexMappingService {
    private val logger = LogManager.getLogger(javaClass)
    private lateinit var client: Client

    private const val TYPE = "type"
    private const val PROPERTIES = "properties"
    private const val METADATA = "_meta"
    private const val SCHEMA_VERSION = "schema_version"
    private const val DYNAMIC_TEMPLATE = "dynamic_templates"
    private const val MATCH_MAPPING_TYPE = "match_mapping_type"
    private const val MAPPING = "mapping"

    fun initialize(client: Client) {
        this.client = client
    }

    /**
     *
     * Check if the source index contains date fields and returns target index mapping for date fields by using default date format
     * Example:
     *   input map: [tpep_pickup_datetime, [type: date]]
     *   target index mapping: "tpep_pickup_datetime": {
     *       "type": "date",
     *       "format": "strict_date_optional_time||epoch_millis"
     *   }
     * @return map of the date properties
     *
     */
    suspend fun getTargetMappingsForDates(transform: Transform): Map<String, Any> {
        val sourceIndex = transform.sourceIndex
        try {
            val result: GetMappingsResponse = client.admin().indices().suspendUntil {
                getMappings(GetMappingsRequest().indices(sourceIndex), it)
            } ?: error("GetMappingResponse for [$transform.sourceIndex] was null")

            val sourceIndexMapping = result.mappings[sourceIndex]?.sourceAsMap

            val targetIndexDateFieldMappings = mutableMapOf<String, Any>()
            if (!sourceIndexMapping.isNullOrEmpty()) {
                mapDateTermAggregation(transform, sourceIndexMapping, targetIndexDateFieldMappings)
                mapDateAggregation(transform.aggregations.aggregatorFactories, sourceIndexMapping, targetIndexDateFieldMappings, null)
            }
            return targetIndexDateFieldMappings
        } catch (ex: IndexNotFoundException) {
            logger.error("Index $sourceIndex doesn't exist")
            return emptyMap()
        }
    }

    private fun mapDateTermAggregation(
        transform: Transform,
        sourceIndexMapping: MutableMap<String, Any>,
        dateFieldMappings: MutableMap<String, Any>,
    ) {
        transform.groups.forEach { dimension ->
            if (!isFieldInMappings(dimension.sourceField, sourceIndexMapping)) {
                throw TransformIndexException("Missing field ${dimension.sourceField} in source index")
            }
            val sourceFieldType = IndexUtils.getFieldFromMappings(dimension.sourceField, sourceIndexMapping)
            // Consider only date fields as relevant for building the target index mapping
            // Excluding date histogram since user can define format in it
            if (dimension !is DateHistogram && isSourceFieldDate(sourceFieldType)) {
                // Taking the source field settings (type, format etc.)
                val dateTypeTargetMapping = mapOf("type" to sourceFieldType!![TYPE], "format" to DEFAULT_DATE_FORMAT)
                dateFieldMappings[dimension.targetField] = dateTypeTargetMapping
            }
        }
    }

    private fun isSourceFieldDate(sourceFieldType: Map<*, *>?) =
        sourceFieldType?.get(TYPE) != null && (sourceFieldType[TYPE] == "date" || sourceFieldType[TYPE] == "date_nanos")

    fun createTargetIndexMapping(dateFieldMappings: Map<String, Any>): String {
        /** TODO - Check if the mappings from file can be loaded into the XContentBuilder which later on can be populated with additional date field mappings
         * val dynamicMappings = IndexManagementIndices.transformTargetMappings
         * val mappings = createTargetIndexMappingsAsString(dateFieldMappings, dynamicMappings)
         */
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
        mapCompositeAggregation(dateFieldMappings, builder)
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

    /**
     * Creates properties section in target index mappings based on the given date fields
     * Parses target index mapping as a string - instead of using XContentBuilder
     */
    private fun createTargetIndexMappingsAsString(
        dateFieldMappings: Map<String, Any>,
        dynamicMappings: String,
    ): String {
        val compositeAgg = mapCompositeAggregationToString(dateFieldMappings)
        return dynamicMappings.trimIndent().dropLast(1) + ", \n \"properties\" : \n { \n $compositeAgg \n } \n }"
    }

    @Suppress("UNCHECKED_CAST")
    private fun mapCompositeAggregationToString(
        compositeAggregation: Map<String, Any>,
    ): String {
        return buildString {
            var isFirst = true
            val iterator = compositeAggregation.entries.iterator()
            while (iterator.hasNext()) {
                val it = iterator.next()
                if (!isFirst) {
                    append(",")
                }
                isFirst = false
                if (it.value is Map<*, *>) {
                    append("\"${it.key}\" : {")
                    append(mapCompositeAggregationToString(it.value as Map<String, Any>))
                    append("\n }")
                } else {
                    append("\n")
                    append("\"${it.key}\" : \"${it.value}\"")
                }
            }
        }
    }

    private fun mapDateAggregation(
        aggBuilders: Collection<AggregationBuilder>,
        sourceIndexMapping: Map<String, Any>,
        targetIndexMapping: MutableMap<String, Any>,
        parentPath: String?,
    ) {
        val iterator = aggBuilders.iterator()
        while (iterator.hasNext()) {

            val aggBuilder = iterator.next()
            val targetIdxFieldName = aggBuilder.name
            val fullPath = parentPath?.plus(".")?.plus(targetIdxFieldName) ?: targetIdxFieldName
            // In the case of a date field used in aggregation - MIN, MAX or COUNT
            if (aggBuilder is ValuesSourceAggregationBuilder<*>) {
                val sourceIdxFieldName = aggBuilder.field()

                val sourceFieldType = IndexUtils.getFieldFromMappings(sourceIdxFieldName, sourceIndexMapping)
                // Consider only aggregations on date field type
                if (!sourceFieldType.isNullOrEmpty() && sourceFieldType[TYPE] == "date") {
                    val dateTypeTargetMapping = mapOf("type" to "date", "format" to DEFAULT_DATE_FORMAT)
                    // In the case if sub-aggregation exist
                    targetIndexMapping[fullPath] = dateTypeTargetMapping
                }
            }
            if (aggBuilder.subAggregations.isNullOrEmpty()) {
                continue
            }
            // Do the same for all sub-aggregations
            mapDateAggregation(aggBuilder.subAggregations, sourceIndexMapping, targetIndexMapping, fullPath)
        }
    }
    private fun isFieldInMappings(fieldName: String, mappings: Map<*, *>) = IndexUtils.getFieldFromMappings(fieldName, mappings) != null
}
