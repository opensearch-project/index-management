/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.opensearchapi.instant
import java.io.IOException
import java.time.Instant

data class ContinuousTransformStats(
    val lastTimestamp: Instant,
    val lastSeqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val lastPrimaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        lastTimestamp = sin.readInstant(),
        lastSeqNo = sin.readLong(),
        lastPrimaryTerm = sin.readLong()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .timeField(LAST_TIMESTAMP_FIELD, LAST_TIMESTAMP_FIELD_IN_MILLIS, lastTimestamp.toEpochMilli())
            .field(LAST_SEQ_NO_FIELD, lastSeqNo)
            .field(LAST_PRIMARY_TERM_FIELD, lastPrimaryTerm)
            .endObject()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeInstant(lastTimestamp)
        out.writeLong(lastSeqNo)
        out.writeLong(lastPrimaryTerm)
    }

    companion object {
        private const val LAST_TIMESTAMP_FIELD = "last_timestamp"
        private const val LAST_TIMESTAMP_FIELD_IN_MILLIS = "last_timestamp_in_millis"
        private const val LAST_SEQ_NO_FIELD = "last_seq_no"
        private const val LAST_PRIMARY_TERM_FIELD = "last_primary_term"

        @Suppress("ComplexMethod, LongMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ContinuousTransformStats {
            var lastTimestamp: Instant? = null
            var lastSeqNo: Long? = null
            var lastPrimaryTerm: Long? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    LAST_TIMESTAMP_FIELD -> lastTimestamp = xcp.instant()
                    LAST_SEQ_NO_FIELD -> lastSeqNo = xcp.longValue()
                    LAST_PRIMARY_TERM_FIELD -> lastPrimaryTerm = xcp.longValue()
                }
            }

            return ContinuousTransformStats(
                lastTimestamp = requireNotNull(lastTimestamp) { "Last timestamp must not be null" },
                lastSeqNo = requireNotNull(lastSeqNo) { "Last sequence number must not be null" },
                lastPrimaryTerm = requireNotNull(lastPrimaryTerm) { "Last primary term must not be null" }
            )
        }
    }
}
