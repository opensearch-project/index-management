/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Metric
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.model.metric.ValueCount
import java.io.IOException

data class RollupMetrics(
    val sourceField: String,
    val targetField: String,
    val metrics: List<Metric>
) : ToXContentObject, Writeable {

    init {
        require(metrics.size == metrics.distinctBy { it.type }.size) {
            "Cannot have multiple metrics of the same type in a single rollup metric [$metrics]"
        }
        require(sourceField.isNotEmpty() && targetField.isNotEmpty()) { "Source and target field must not be empty" }
        require(metrics.isNotEmpty()) { "Must specify at least one metric to aggregate on for $sourceField" }
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sourceField = sin.readString(),
        targetField = sin.readString(),
        metrics = sin.let {
            val metricsList = mutableListOf<Metric>()
            val size = it.readVInt()
            repeat(size) { _ ->
                val type = it.readEnum(Metric.Type::class.java)
                metricsList.add(
                    when (requireNotNull(type) { "Metric type cannot be null" }) {
                        Metric.Type.AVERAGE -> Average(it)
                        Metric.Type.MAX -> Max(it)
                        Metric.Type.MIN -> Min(it)
                        Metric.Type.SUM -> Sum(it)
                        Metric.Type.VALUE_COUNT -> ValueCount(it)
                    }
                )
            }
            metricsList.toList()
        }
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(SOURCE_FIELD_FIELD, sourceField)
            .field(METRICS_FIELD, metrics.toTypedArray())
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(sourceField)
        out.writeString(targetField)
        out.writeVInt(metrics.size)
        for (metric in metrics) {
            out.writeEnum(metric.type)
            when (metric) {
                is Average -> metric.writeTo(out)
                is Max -> metric.writeTo(out)
                is Min -> metric.writeTo(out)
                is Sum -> metric.writeTo(out)
                is ValueCount -> metric.writeTo(out)
            }
        }
    }

    fun targetFieldWithType(metric: Metric): String {
        return when (metric) {
            is Average -> "$targetField.avg"
            is Sum -> "$targetField.sum"
            is Max -> "$targetField.max"
            is Min -> "$targetField.min"
            is ValueCount -> "$targetField.value_count"
            else -> throw IllegalArgumentException("Found unsupported metric aggregation ${metric.type.type}")
        }
    }

    companion object {
        const val SOURCE_FIELD_FIELD = "source_field"
        const val TARGET_FIELD_FIELD = "target_field"
        const val METRICS_FIELD = "metrics"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): RollupMetrics {
            var sourceField: String? = null
            var targetField: String? = null
            val metrics = mutableListOf<Metric>()

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    SOURCE_FIELD_FIELD -> sourceField = xcp.text()
                    TARGET_FIELD_FIELD -> targetField = xcp.text()
                    METRICS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            metrics.add(Metric.parse(xcp))
                        }
                    }
                    else -> throw IllegalArgumentException("Invalid dimension type [$fieldName] found in rollup metrics")
                }
            }

            if (targetField == null) targetField = sourceField
            return RollupMetrics(
                sourceField = requireNotNull(sourceField) { "Source field must not be null" },
                targetField = requireNotNull(targetField) { "Target field must not be null" },
                metrics = metrics.toList()
            )
        }
    }
}
