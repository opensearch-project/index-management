/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.opensearch.client.Client
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.transform.exceptions.TransformIndexException
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.util.DEFAULT_DATE_FORMAT
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.search.aggregations.AggregationBuilder
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

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
    private val DATE_FIELD_TYPES = setOf("date", "date_nanos")

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
            val result: GetMappingsResponse =
                client.admin().indices().suspendUntil {
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
        sourceFieldType?.get(TYPE) != null && DATE_FIELD_TYPES.contains(sourceFieldType[TYPE])

    /**
     * Loads transform target index mappings from json and adds date properties mapping
     *
     * @param dateFieldMappings target index date fields mappings
     */
    fun createTargetIndexMapping(dateFieldMappings: Map<String, Any>): String {
        val builder = XContentFactory.jsonBuilder()
        val dynamicMappings = IndexManagementIndices.transformTargetMappings
        val byteBuffer = ByteBuffer.wrap(dynamicMappings.toByteArray(StandardCharsets.UTF_8))
        val bytesReference = BytesReference.fromByteBuffer(byteBuffer)

        val xcp =
            XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                bytesReference,
                XContentType.JSON,
            )
        loop@while (!xcp.isClosed) {
            val token = xcp.currentToken()
            val fieldName = xcp.currentName()

            when (token) {
                XContentParser.Token.VALUE_NUMBER -> builder.field(fieldName, xcp.intValue())
                XContentParser.Token.VALUE_STRING -> builder.field(fieldName, xcp.text())
                XContentParser.Token.START_OBJECT -> {
                    if (fieldName != null) {
                        builder.startObject(fieldName)
                    } else {
                        builder.startObject()
                    }
                }
                XContentParser.Token.END_OBJECT -> builder.endObject()
                XContentParser.Token.START_ARRAY -> builder.startArray(fieldName)
                XContentParser.Token.END_ARRAY -> {
                    builder.endArray()
                    // Add target index date fields mappings only if the date field mappings are present
                    if (dateFieldMappings.isNotEmpty()) {
                        builder.startObject(PROPERTIES)
                        mapCompositeAggregation(dateFieldMappings, builder)
                        builder.endObject()
                    }
                }
                else -> {
                    xcp.nextToken()
                    continue@loop
                }
            }
            xcp.nextToken()
        }
        return builder.string()
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
                if (DATE_FIELD_TYPES.contains(it.value)) {
                    builder.field(it.key, it.value.toString())
                }
            }
        }
    }

    /**
     * Creates properties section in target index mappings based on the given date fields
     * Parses target index mapping as a string - instead of using XContentBuilder
     */
    @Suppress("UNUSED_PARAMETER")
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
