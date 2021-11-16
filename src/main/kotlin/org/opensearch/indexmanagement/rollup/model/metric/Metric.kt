/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model.metric

import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

abstract class Metric(val type: Type) : ToXContentObject, Writeable {

    enum class Type(val type: String) {
        AVERAGE("avg"),
        SUM("sum"),
        MAX("max"),
        MIN("min"),
        VALUE_COUNT("value_count");

        override fun toString(): String {
            return type
        }
    }

    companion object {
        @Suppress("ComplexMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Metric {
            var metric: Metric? = null
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                metric = when (fieldName) {
                    Type.AVERAGE.type -> Average.parse(xcp)
                    Type.MAX.type -> Max.parse(xcp)
                    Type.MIN.type -> Min.parse(xcp)
                    Type.SUM.type -> Sum.parse(xcp)
                    Type.VALUE_COUNT.type -> ValueCount.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid metric type [$fieldName] found in metrics")
                }
            }

            return requireNotNull(metric) { "Metric is null" }
        }
    }
}
