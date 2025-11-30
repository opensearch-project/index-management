/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.model

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.util.NO_ID
import java.io.IOException

data class LRONCondition(
    val success: Boolean = DEFAULT_ENABLED,
    val failure: Boolean = DEFAULT_ENABLED,
) : ToXContentObject,
    Writeable {
    fun toXContent(builder: XContentBuilder): XContentBuilder = toXContent(builder, ToXContent.EMPTY_PARAMS)

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder = builder.startObject()
        .field(SUCCESS_FIELD, success)
        .field(FAILURE_FIELD, failure)
        .endObject()

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        success = sin.readBoolean(),
        failure = sin.readBoolean(),
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(success)
        out.writeBoolean(failure)
    }

    fun isEnabled(): Boolean = success || failure

    companion object {
        const val SUCCESS_FIELD = "success"
        const val FAILURE_FIELD = "failure"
        const val LRON_CONDITION_FIELD = "lron_condition"
        private const val DEFAULT_ENABLED = true

        @JvmStatic
        @Throws(IOException::class)
        @Suppress("UNUSED_PARAMETER")
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
        ): LRONCondition = parse(xcp)

        @JvmStatic
        @Suppress("MaxLineLength", "CyclomaticComplexMethod", "NestedBlockDepth")
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): LRONCondition {
            var success: Boolean = DEFAULT_ENABLED
            var failure: Boolean = DEFAULT_ENABLED

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    SUCCESS_FIELD -> success = xcp.booleanValue()
                    FAILURE_FIELD -> failure = xcp.booleanValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in LRONCondition.")
                }
            }

            return LRONCondition(
                success = success,
                failure = failure,
            )
        }
    }
}
