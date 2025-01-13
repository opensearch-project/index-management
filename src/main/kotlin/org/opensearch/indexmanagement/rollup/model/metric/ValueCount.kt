/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.model.metric

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken

class ValueCount() : Metric(Type.VALUE_COUNT) {
    @Suppress("UNUSED_PARAMETER")
    constructor(sin: StreamInput) : this()

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder = builder.startObject().startObject(Type.VALUE_COUNT.type).endObject().endObject()

    override fun writeTo(out: StreamOutput) { /* nothing to write */ }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int = javaClass.hashCode()

    override fun toString(): String = "ValueCount()"

    companion object {
        fun parse(xcp: XContentParser): ValueCount {
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp)
            return ValueCount()
        }
    }
}
