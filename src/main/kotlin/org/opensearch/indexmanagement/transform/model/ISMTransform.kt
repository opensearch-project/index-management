/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.apache.commons.codec.digest.DigestUtils
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.authuser.User
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.AbstractQueryBuilder
import org.opensearch.index.query.MatchAllQueryBuilder
import org.opensearch.index.query.QueryBuilder
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.common.model.dimension.Histogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.search.aggregations.AggregatorFactories
import java.io.IOException
import java.lang.StringBuilder
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.jvm.Throws

data class ISMTransform(
    val description: String,
    val targetIndex: String,
    val pageSize: Int,
    val dataSelectionQuery: QueryBuilder = MatchAllQueryBuilder(),
    val groups: List<Dimension>,
    val aggregations: AggregatorFactories.Builder = AggregatorFactories.builder(),
) : ToXContentObject,
    Writeable {

    init {
        require(pageSize in Transform.MINIMUM_PAGE_SIZE..Transform.MAXIMUM_PAGE_SIZE) {
            "Page size must be between ${Transform.MINIMUM_PAGE_SIZE} and ${Transform.MAXIMUM_PAGE_SIZE}"
        }
        require(description.isNotEmpty()) { "Description cannot be empty" }
        require(targetIndex.isNotEmpty()) { "TargetIndex cannot be empty" }
        require(groups.isNotEmpty()) { "Groups cannot be empty" }
        aggregations.aggregatorFactories.forEach {
            require(Transform.supportedAggregations.contains(it.type)) {
                "Unsupported aggregation [${it.type}]"
            }
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        builder.startObject()
            .field(Transform.DESCRIPTION_FIELD, description)
            .field(Transform.TARGET_INDEX_FIELD, targetIndex)
            .field(Transform.PAGE_SIZE_FIELD, pageSize)
            .field(Transform.DATA_SELECTION_QUERY_FIELD, dataSelectionQuery)
            .field(Transform.GROUPS_FIELD, groups)
            .field(Transform.AGGREGATIONS_FIELD, aggregations)
        builder.endObject()
        return builder
    }

    fun toTransform(sourceIndex: String, user: User? = null): Transform {
        val id = sourceIndex + toString()
        val currentTime = Instant.now()
        return Transform(
            id = DigestUtils.sha1Hex(id),
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            schemaVersion = IndexUtils.DEFAULT_SCHEMA_VERSION,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            metadataId = null,
            updatedAt = currentTime,
            enabled = true,
            enabledAt = currentTime,
            description = this.description,
            sourceIndex = sourceIndex,
            dataSelectionQuery = this.dataSelectionQuery,
            targetIndex = this.targetIndex,
            pageSize = pageSize,
            continuous = false,
            groups = this.groups,
            aggregations = this.aggregations,
            user = user,
        )
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        description = sin.readString(),
        targetIndex = sin.readString(),
        pageSize = sin.readInt(),
        dataSelectionQuery = requireNotNull(sin.readOptionalNamedWriteable(QueryBuilder::class.java)) { "Query cannot be null" },
        groups = sin.let {
            val dimensionList = mutableListOf<Dimension>()
            val size = it.readVInt()
            repeat(size) { _ ->
                val type = it.readEnum(Dimension.Type::class.java)
                dimensionList.add(
                    when (requireNotNull(type) { "Dimension type cannot be null" }) {
                        Dimension.Type.DATE_HISTOGRAM -> DateHistogram(sin)
                        Dimension.Type.TERMS -> Terms(sin)
                        Dimension.Type.HISTOGRAM -> Histogram(sin)
                    },
                )
            }
            dimensionList.toList()
        },
        aggregations = requireNotNull(sin.readOptionalWriteable { AggregatorFactories.Builder(it) }) { "Aggregations cannot be null" },
    )

    override fun toString(): String {
        val sbd = StringBuilder()
        sbd.append(targetIndex)
        sbd.append(pageSize)
        sbd.append(dataSelectionQuery)
        groups.forEach {
            sbd.append(it.type)
            sbd.append(it.sourceField)
        }
        sbd.append(aggregations)

        return sbd.toString()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(description)
        out.writeString(targetIndex)
        out.writeInt(pageSize)
        out.writeOptionalNamedWriteable(dataSelectionQuery)
        out.writeVInt(groups.size)
        for (group in groups) {
            out.writeEnum(group.type)
            when (group) {
                is DateHistogram -> group.writeTo(out)
                is Terms -> group.writeTo(out)
                is Histogram -> group.writeTo(out)
            }
        }
        out.writeOptionalWriteable(aggregations)
    }

    companion object {
        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ISMTransform {
            var description = ""
            var targetIndex = ""
            var pageSize = 0
            var dataSelectionQuery: QueryBuilder = MatchAllQueryBuilder()
            val groups = mutableListOf<Dimension>()
            var aggregations: AggregatorFactories.Builder = AggregatorFactories.builder()

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)

            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    Transform.DESCRIPTION_FIELD -> description = xcp.text()
                    Transform.TARGET_INDEX_FIELD -> targetIndex = xcp.text()
                    Transform.PAGE_SIZE_FIELD -> pageSize = xcp.intValue()
                    Transform.DATA_SELECTION_QUERY_FIELD -> {
                        val registry = xcp.xContentRegistry
                        val source = xcp.mapOrdered()
                        val xContentBuilder = XContentFactory.jsonBuilder().map(source)
                        val sourceParser = XContentType.JSON.xContent().createParser(
                            registry, LoggingDeprecationHandler.INSTANCE,
                            BytesReference
                                .bytes(xContentBuilder).streamInput(),
                        )
                        dataSelectionQuery = AbstractQueryBuilder.parseInnerQueryBuilder(sourceParser)
                    }
                    Transform.GROUPS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            groups.add(Dimension.parse(xcp))
                        }
                    }
                    Transform.AGGREGATIONS_FIELD -> aggregations = AggregatorFactories.parseAggregators(xcp)
                    else -> throw IllegalArgumentException("Invalid field [$fieldName] found in ISM Transform.")
                }
            }

            return ISMTransform(
                description = description,
                targetIndex = targetIndex,
                pageSize = pageSize,
                dataSelectionQuery = dataSelectionQuery,
                groups = groups,
                aggregations = aggregations,
            )
        }
    }
}
