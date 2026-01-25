/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.apache.commons.codec.digest.DigestUtils
import org.opensearch.Version
import org.opensearch.common.settings.Settings
import org.opensearch.commons.authuser.User
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.common.model.dimension.Histogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import java.io.IOException
import java.time.Instant
import java.time.temporal.ChronoUnit

data class ISMRollup(
    val description: String,
    val targetIndex: String,
    val targetIndexSettings: Settings?,
    val pageSize: Int,
    val dimensions: List<Dimension>,
    val metrics: List<RollupMetrics>,
    val sourceIndex: String? = null,
) : ToXContentObject,
    Writeable {
    // TODO: This can be moved to a common place, since this is shared between Rollup and ISMRollup
    init {
        require(pageSize in Rollup.MINIMUM_PAGE_SIZE..Rollup.MAXIMUM_PAGE_SIZE) {
            "Page size must be between ${Rollup.MINIMUM_PAGE_SIZE} " +
                "and ${Rollup.MAXIMUM_PAGE_SIZE}"
        }
        require(description.isNotEmpty()) { "Description cannot be empty" }
        require(targetIndex.isNotEmpty()) { "Target Index cannot be empty" }
        require(dimensions.filter { it.type == Dimension.Type.DATE_HISTOGRAM }.size == 1) {
            "Must specify precisely one date histogram dimension"
        }
        require(dimensions.first().type == Dimension.Type.DATE_HISTOGRAM) { "The first dimension must be a date histogram" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(Rollup.DESCRIPTION_FIELD, description)
            .field(Rollup.TARGET_INDEX_FIELD, targetIndex)
            .field(Rollup.PAGE_SIZE_FIELD, pageSize)
            .field(Rollup.DIMENSIONS_FIELD, dimensions)
            .field(Rollup.METRICS_FIELD, metrics)
        if (sourceIndex != null) {
            builder.field(Rollup.SOURCE_INDEX_FIELD, sourceIndex)
        }
        if (targetIndexSettings != null) {
            builder.startObject(Rollup.TARGET_INDEX_SETTINGS_FIELD)
            targetIndexSettings.toXContent(builder, params)
            builder.endObject()
        }
        builder.endObject()
        return builder
    }

    /**
     * Converts ISMRollup configuration to a Rollup job.
     *
     * @param sourceIndex The managed index from ISM context (fallback if ISMRollup.sourceIndex is null)
     * @param user Optional user context for the rollup job
     * @return Rollup job configuration
     */
    fun toRollup(sourceIndex: String, user: User? = null): Rollup {
        val resolvedSourceIndex = this.sourceIndex ?: sourceIndex
        val id = resolvedSourceIndex + toString()
        val currentTime = Instant.now()
        return Rollup(
            id = DigestUtils.sha1Hex(id),
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            enabled = true,
            schemaVersion = IndexUtils.DEFAULT_SCHEMA_VERSION,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = currentTime,
            jobEnabledTime = currentTime,
            description = this.description,
            sourceIndex = resolvedSourceIndex,
            targetIndex = this.targetIndex,
            targetIndexSettings = this.targetIndexSettings,
            metadataID = null,
            pageSize = pageSize,
            delay = null,
            continuous = false,
            dimensions = dimensions,
            metrics = metrics,
            user = user,
        )
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        description = sin.readString(),
        targetIndex = sin.readString(),
        targetIndexSettings = if (sin.version.onOrAfter(Version.V_3_0_0) && sin.readBoolean()) {
            Settings.readSettingsFromStream(sin)
        } else {
            null
        },
        pageSize = sin.readInt(),
        dimensions =
        sin.let {
            val dimensionsList = mutableListOf<Dimension>()
            val size = it.readVInt()
            repeat(size) { _ ->
                val type = it.readEnum(Dimension.Type::class.java)
                dimensionsList.add(
                    when (requireNotNull(type) { "Dimension type cannot be null" }) {
                        Dimension.Type.DATE_HISTOGRAM -> DateHistogram(sin)
                        Dimension.Type.TERMS -> Terms(sin)
                        Dimension.Type.HISTOGRAM -> Histogram(sin)
                    },
                )
            }
            dimensionsList.toList()
        },
        metrics = sin.readList(::RollupMetrics),
        sourceIndex = if (sin.version.onOrAfter(Version.V_3_5_0) && sin.readBoolean()) {
            sin.readString()
        } else {
            null
        },
    )

    override fun toString(): String {
        val sb = StringBuffer()
        sb.append(targetIndex)
        sb.append(targetIndexSettings)
        sb.append(pageSize)
        dimensions.forEach {
            sb.append(it.type)
            sb.append(it.sourceField)
        }
        metrics.forEach {
            sb.append(it.sourceField)
            it.metrics.forEach { metric ->
                sb.append(metric.type)
            }
        }

        return sb.toString()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(description)
        out.writeString(targetIndex)
        if (out.version.onOrAfter(Version.V_3_0_0)) {
            out.writeBoolean(targetIndexSettings != null)
            if (targetIndexSettings != null) Settings.writeSettingsToStream(targetIndexSettings, out)
        }
        out.writeInt(pageSize)
        out.writeVInt(dimensions.size)
        for (dimension in dimensions) {
            out.writeEnum(dimension.type)
            when (dimension) {
                is DateHistogram -> dimension.writeTo(out)
                is Terms -> dimension.writeTo(out)
                is Histogram -> dimension.writeTo(out)
            }
        }
        out.writeCollection(metrics)
        if (out.version.onOrAfter(Version.V_3_5_0)) {
            out.writeBoolean(sourceIndex != null)
            if (sourceIndex != null) out.writeString(sourceIndex)
        }
    }

    companion object {
        @Suppress("CyclomaticComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
        ): ISMRollup {
            var description = ""
            var targetIndex = ""
            var sourceIndex: String? = null
            var targetIndexSettings: Settings? = null
            var pageSize = 0
            val dimensions = mutableListOf<Dimension>()
            val metrics = mutableListOf<RollupMetrics>()

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)

            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    Rollup.DESCRIPTION_FIELD -> description = xcp.text()

                    Rollup.TARGET_INDEX_FIELD -> targetIndex = xcp.text()

                    Rollup.SOURCE_INDEX_FIELD -> sourceIndex = xcp.text()

                    Rollup.TARGET_INDEX_SETTINGS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(
                            XContentParser.Token.START_OBJECT,
                            xcp.currentToken(),
                            xcp,
                        )
                        targetIndexSettings = Settings.fromXContent(xcp)
                    }

                    Rollup.PAGE_SIZE_FIELD -> pageSize = xcp.intValue()

                    Rollup.DIMENSIONS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(
                            XContentParser.Token.START_ARRAY,
                            xcp.currentToken(),
                            xcp,
                        )
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            dimensions.add(Dimension.parse(xcp))
                        }
                    }

                    Rollup.METRICS_FIELD -> {
                        XContentParserUtils.ensureExpectedToken(
                            XContentParser.Token.START_ARRAY,
                            xcp.currentToken(),
                            xcp,
                        )
                        while (xcp.nextToken() != XContentParser.Token.END_ARRAY) {
                            metrics.add(RollupMetrics.parse(xcp))
                        }
                    }

                    else -> throw IllegalArgumentException("Invalid field, [$fieldName] not supported in ISM Rollup.")
                }
            }

            return ISMRollup(
                description = description,
                pageSize = pageSize,
                dimensions = dimensions,
                metrics = metrics,
                targetIndex = targetIndex,
                targetIndexSettings = targetIndexSettings,
                sourceIndex = sourceIndex,
            )
        }
    }
}
