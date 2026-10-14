/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.fielddomain

import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.index.fielddomain.DateRangeFieldDomain
import org.opensearch.index.fielddomain.FieldDomain
import org.opensearch.index.mapper.DateFieldMapper
import org.opensearch.index.query.QueryBuilders
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
        context: FieldDomainSearchContext,
        indexMetadata: IndexMetadata,
        config: FieldDomainConfig,
        finalized: Boolean,
    ): FieldDomain? {
        val fieldMapping = getFieldMapping(indexMetadata, config.field)
        val mappedType = fieldMapping[TYPE_FIELD] as? String
            ?: throw IllegalArgumentException("Field [${config.field}] does not declare a mapping type")
        val dateMetadata = resolveDateMetadata(config.field, mappedType)
        val boundaries = findBoundaries(context, config, dateMetadata)

        return if (boundaries == null) {
            null
        } else {
            val format = fieldMapping[FORMAT_FIELD] as? String
            DateRangeFieldDomain(
                config.field,
                boundaries.min.toString(),
                boundaries.max.toString(),
                finalized,
                SOURCE,
                format,
                dateMetadata.resolution,
            )
        }
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

    private suspend fun findBoundaries(
        context: FieldDomainSearchContext,
        config: FieldDomainConfig,
        dateMetadata: DateMetadata,
    ): Boundaries? {
        // Write block and refresh make min and max boundaries stable across both searches.
        val min = findBoundary(
            context = context,
            field = config.field,
            numericType = dateMetadata.numericType,
            order = SortOrder.ASC,
            sortMode = SortMode.MIN,
        )

        val max = min?.let {
            findBoundary(
                context = context,
                field = config.field,
                numericType = dateMetadata.numericType,
                order = SortOrder.DESC,
                sortMode = SortMode.MAX,
            )
        }

        return if (min == null || max == null) null else Boundaries(min, max)
    }

    private suspend fun findBoundary(
        context: FieldDomainSearchContext,
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

        val searchResponse = context.search(searchSource)
        val hits = searchResponse.hits.hits
        return if (hits.isEmpty()) null else rawSortValueAsLong(field, hits[0].rawSortValues)
    }

    private fun rawSortValueAsLong(field: String, rawSortValues: Array<Any>): Long {
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

    private data class Boundaries(
        val min: Long,
        val max: Long,
    )

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
