/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.fielddomain

import org.opensearch.action.search.SearchRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.index.fielddomain.DateRangeFieldDomain
import org.opensearch.index.fielddomain.FieldDomain
import org.opensearch.index.mapper.DateFieldMapper
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortMode
import org.opensearch.search.sort.SortOrder
import java.util.Locale

/**
 * Computes date/date_nanos field domains using sorted first/last hits.
 *
 * The calculator intentionally reads raw sort values instead of aggregation output. Raw field-sort values preserve the
 * requested numeric date resolution: epoch milliseconds for {@code date} and epoch nanoseconds for {@code date_nanos}.
 */
class DateRangeFieldDomainCalculator : FieldDomainCalculator {
    override val type: String = DateRangeFieldDomain.TYPE

    override suspend fun calculate(
        context: StepContext,
        indexMetadata: IndexMetadata,
        config: FieldDomainConfig,
        finalized: Boolean,
    ): FieldDomain? {
        val fieldMapping = getFieldMapping(indexMetadata, config.field)
        val mappedType = fieldMapping[TYPE_FIELD] as? String
            ?: throw IllegalArgumentException("Field [${config.field}] does not declare a mapping type")
        val dateMetadata = resolveDateMetadata(config.field, mappedType)

        // Write block and refresh make min and max boundaries stable across both searches.
        val min = findBoundary(
            context = context,
            indexName = indexMetadata.index.name,
            field = config.field,
            numericType = dateMetadata.numericType,
            order = SortOrder.ASC,
            sortMode = SortMode.MIN,
        ) ?: return null

        val max = findBoundary(
            context = context,
            indexName = indexMetadata.index.name,
            field = config.field,
            numericType = dateMetadata.numericType,
            order = SortOrder.DESC,
            sortMode = SortMode.MAX,
        ) ?: return null

        val format = fieldMapping[FORMAT_FIELD] as? String
        return DateRangeFieldDomain(
            config.field,
            min.toString(),
            max.toString(),
            finalized,
            SOURCE,
            format,
            dateMetadata.resolution,
        )
    }

    private fun getFieldMapping(indexMetadata: IndexMetadata, field: String): Map<*, *> {
        val mapping = indexMetadata.mapping()?.sourceAsMap()
            ?: throw IllegalArgumentException("Index [${indexMetadata.index.name}] does not have mappings")
        return IndexUtils.getFieldFromMappings(field, mapping)
            ?: throw IllegalArgumentException("Field [$field] is not present in index [${indexMetadata.index.name}] mappings")
    }

    private fun resolveDateMetadata(field: String, mappedType: String): DateMetadata = when (mappedType) {
        DateFieldMapper.CONTENT_TYPE -> DateMetadata(
            numericType = DateFieldMapper.CONTENT_TYPE,
            resolution = DateFieldMapper.Resolution.MILLISECONDS.name.lowercase(Locale.ROOT),
        )

        DateFieldMapper.DATE_NANOS_CONTENT_TYPE -> DateMetadata(
            numericType = DateFieldMapper.DATE_NANOS_CONTENT_TYPE,
            resolution = DateFieldMapper.Resolution.NANOSECONDS.name.lowercase(Locale.ROOT),
        )

        else -> throw IllegalArgumentException(
            "Field [$field] must be mapped as [${DateFieldMapper.CONTENT_TYPE}] or " +
                "[${DateFieldMapper.DATE_NANOS_CONTENT_TYPE}], found [$mappedType]",
        )
    }

    private suspend fun findBoundary(
        context: StepContext,
        indexName: String,
        field: String,
        numericType: String,
        order: SortOrder,
        sortMode: SortMode,
    ): Long? {
        val searchSource =
            SearchSourceBuilder.searchSource()
                .query(QueryBuilders.existsQuery(field))
                .fetchSource(false)
                .size(1)
                .sort(
                    SortBuilders.fieldSort(field)
                        .order(order)
                        .sortMode(sortMode)
                        .setNumericType(numericType),
                )

        val searchRequest =
            SearchRequest(indexName)
                .source(searchSource)
                .indicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .allowPartialSearchResults(false)

        val searchResponse = context.client.suspendUntil { search(searchRequest, it) }
        if (searchResponse.shardFailures.isNotEmpty()) {
            throw IllegalStateException(
                "Failed to compute field domain for [$field] in index [$indexName], " +
                    "shard failures [${searchResponse.shardFailures.joinToString { it.reason() }}]",
            )
        }

        val hits = searchResponse.hits.hits
        if (hits.isEmpty()) {
            return null
        }

        val rawSortValues = hits[0].rawSortValues
        if (rawSortValues.size != 1) {
            throw IllegalStateException("Expected one raw sort value for field [$field], found [${rawSortValues.size}]")
        }

        return when (val rawValue = rawSortValues[0]) {
            is Long -> rawValue

            is Number -> rawValue.toLong()

            else -> throw IllegalStateException(
                "Expected numeric raw sort value for field [$field], found [${rawValue?.javaClass?.name}]",
            )
        }
    }

    private data class DateMetadata(
        val numericType: String,
        val resolution: String,
    )

    companion object {
        private const val TYPE_FIELD = "type"
        private const val FORMAT_FIELD = "format"
        private const val SOURCE = "ism"
    }
}
