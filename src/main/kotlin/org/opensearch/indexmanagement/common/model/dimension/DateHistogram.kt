/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.common.model.dimension

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.query.AbstractQueryBuilder
import org.opensearch.index.query.RangeQueryBuilder
import org.opensearch.indexmanagement.util.IndexUtils.Companion.getFieldFromMappings
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval
import java.io.IOException
import java.time.ZoneId

data class DateHistogram(
    override val sourceField: String,
    override val targetField: String = sourceField,
    val fixedInterval: String? = null,
    val calendarInterval: String? = null,
    val timezone: ZoneId = ZoneId.of(UTC)
) : Dimension(Type.DATE_HISTOGRAM, sourceField, targetField) {

    init {
        require(sourceField.isNotEmpty() && targetField.isNotEmpty()) { "Source and target field must not be empty" }
        require(fixedInterval != null || calendarInterval != null) { "Must specify a fixed or calendar interval" }
        require(fixedInterval == null || calendarInterval == null) { "Can only specify a fixed or calendar interval" }
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sourceField = sin.readString(),
        targetField = sin.readString(),
        fixedInterval = sin.readOptionalString(),
        calendarInterval = sin.readOptionalString(),
        timezone = sin.readZoneId()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .startObject(type.type)
        if (fixedInterval != null) builder.field(FIXED_INTERVAL_FIELD, fixedInterval)
        if (calendarInterval != null) builder.field(CALENDAR_INTERVAL_FIELD, calendarInterval)
        return builder.field(DIMENSION_SOURCE_FIELD_FIELD, sourceField)
            .field(DIMENSION_TARGET_FIELD_FIELD, targetField)
            .field(DATE_HISTOGRAM_TIMEZONE_FIELD, timezone.id)
            .endObject()
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(sourceField)
        out.writeString(targetField)
        out.writeOptionalString(fixedInterval)
        out.writeOptionalString(calendarInterval)
        out.writeZoneId(timezone)
    }

    override fun toSourceBuilder(appendType: Boolean): CompositeValuesSourceBuilder<*> {
        val calendarInterval = this.calendarInterval
        val fixedInterval = this.fixedInterval
        val name = if (appendType) "${this.targetField}.${Type.DATE_HISTOGRAM.type}" else this.targetField

        return DateHistogramValuesSourceBuilder(name)
            .missingBucket(true)
            .field(this.sourceField)
            .timeZone(this.timezone)
            .apply {
                calendarInterval?.let {
                    this.calendarInterval(DateHistogramInterval(it))
                }
                fixedInterval?.let {
                    this.fixedInterval(DateHistogramInterval(it))
                }
            }
    }

    override fun toBucketQuery(bucketKey: Any): AbstractQueryBuilder<*> {
        if (bucketKey !is Long) {
            throw IllegalArgumentException("Received invalid date histogram bucket key type [${bucketKey::class}] when Long is expected.")
        }
        val interval: Long = DateHistogramInterval(fixedInterval ?: calendarInterval).estimateMillis()
        return RangeQueryBuilder(sourceField)
            .from(bucketKey, true)
            .to(bucketKey + interval, false)
            .timeZone(timezone.toString())
            .format("epoch_millis")
    }

    override fun canBeRealizedInMappings(mappings: Map<String, Any>): Boolean {
        val fieldType = getFieldFromMappings(sourceField, mappings)?.get("type") ?: return false
        return "date" == fieldType
    }

    fun getRewrittenAggregation(
        aggregationBuilder: DateHistogramAggregationBuilder,
        subAggregations: AggregatorFactories.Builder
    ): DateHistogramAggregationBuilder =
        DateHistogramAggregationBuilder(aggregationBuilder.name)
            .also { aggregationBuilder.calendarInterval?.apply { it.calendarInterval(this) } }
            .also { aggregationBuilder.fixedInterval?.apply { it.fixedInterval(this) } }
            .also { aggregationBuilder.extendedBounds()?.apply { it.extendedBounds(this) } }
            .keyed(aggregationBuilder.keyed())
            .also {
                if (aggregationBuilder.minDocCount() >= 0) {
                    it.minDocCount(aggregationBuilder.minDocCount())
                }
            }
            .offset(aggregationBuilder.offset())
            .also { aggregationBuilder.order()?.apply { it.order(this) } }
            .field(this.targetField + ".date_histogram")
            // TODO: Should we reject all other timezones if they are specified in the query?: aggregationBuilder.timeZone()
            .timeZone(timezone)
            .subAggregations(subAggregations)

    companion object {
        const val UTC = "UTC"
        const val FIXED_INTERVAL_FIELD = "fixed_interval"
        const val CALENDAR_INTERVAL_FIELD = "calendar_interval"
        const val DATE_HISTOGRAM_TIMEZONE_FIELD = "timezone"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): DateHistogram {
            var sourceField: String? = null
            var targetField: String? = null
            var fixedInterval: String? = null
            var calendarInterval: String? = null
            var timezone = ZoneId.of(UTC)

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    FIXED_INTERVAL_FIELD -> fixedInterval = xcp.text()
                    CALENDAR_INTERVAL_FIELD -> calendarInterval = xcp.text()
                    DATE_HISTOGRAM_TIMEZONE_FIELD -> timezone = ZoneId.of(xcp.text())
                    DIMENSION_SOURCE_FIELD_FIELD -> sourceField = xcp.text()
                    DIMENSION_TARGET_FIELD_FIELD -> targetField = xcp.text()
                    else -> throw IllegalArgumentException("Invalid field [$fieldName] found in date histogram")
                }
            }
            if (targetField == null) targetField = sourceField
            return DateHistogram(
                sourceField = requireNotNull(sourceField) { "Source field must not be null" },
                targetField = requireNotNull(targetField) { "Target field must not be null" },
                fixedInterval = fixedInterval,
                calendarInterval = calendarInterval,
                timezone = timezone
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput) = DateHistogram(sin)
    }
}
