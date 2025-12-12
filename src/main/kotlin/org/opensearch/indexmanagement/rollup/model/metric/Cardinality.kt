/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model.metric

import org.apache.lucene.util.packed.PackedInts
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLog

class Cardinality(
    val precisionThreshold: Long = DEFAULT_PRECISION_THRESHOLD,
) : Metric(Type.CARDINALITY) {
    init {
        require(precisionThreshold > 0) {
            "Precision threshold must be positive, got: $precisionThreshold"
        }
    }

    constructor(sin: StreamInput) : this(
        precisionThreshold = sin.readVLong(),
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject().startObject(Type.CARDINALITY.type)
        if (precisionThreshold != DEFAULT_PRECISION_THRESHOLD) {
            builder.field(PRECISION_THRESHOLD_FIELD, precisionThreshold)
        }
        return builder.endObject().endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeVLong(precisionThreshold)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as Cardinality
        return precisionThreshold == other.precisionThreshold
    }

    override fun hashCode(): Int = precisionThreshold.hashCode()

    override fun toString(): String = "Cardinality(precisionThreshold=$precisionThreshold)"

    companion object {
        // Default precision threshold of 3000 provides a good balance between accuracy and memory
        // This maps to precision 14, giving ~0.2% error with ~8KB per sketch
        const val MAX_LOAD_FACTOR: Float = 0.75f
        const val DEFAULT_PRECISION_THRESHOLD = 3000L
        const val PRECISION_THRESHOLD_FIELD = "precision_threshold"

        /**
         * Compute the required precision so that `count` distinct entries would be counted with linear counting.
         */
        fun precisionFromThreshold(count: Long): Int {
            val hashTableEntries = Math.ceil(count / MAX_LOAD_FACTOR.toDouble()).toLong()
            var precision = PackedInts.bitsRequired(hashTableEntries * Integer.BYTES)

            precision = maxOf(precision, AbstractHyperLogLog.MIN_PRECISION)
            precision = minOf(precision, AbstractHyperLogLog.MAX_PRECISION)

            return precision
        }

        fun parse(xcp: XContentParser): Cardinality {
            var precisionThreshold = DEFAULT_PRECISION_THRESHOLD

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    PRECISION_THRESHOLD_FIELD -> precisionThreshold = xcp.longValue()
                    else -> throw IllegalArgumentException("Invalid field [$fieldName] found in cardinality metric")
                }
            }

            return Cardinality(precisionThreshold)
        }
    }
}
