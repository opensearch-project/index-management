/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.apache.logging.log4j.LogManager
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.opensearchapi.instant
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import org.opensearch.indexmanagement.opensearchapi.parseArray
import org.opensearch.indexmanagement.util.NO_ID
import java.time.Instant

data class SMMetadata(
    val policySeqNo: Long,
    val policyPrimaryTerm: Long,
    val currentState: String, // TODO maybe change to SMState is better
    val apiCalling: Boolean = false,
    val nextCreationTime: Instant? = null,
    val creating: String? = null,
    val nextDeletionTime: Instant? = null,
    val deleting: List<String>? = null,
    val info: Map<String, Any>? = null,
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
) : Writeable, ToXContentFragment {

    private val log = LogManager.getLogger(javaClass)

    override fun writeTo(out: StreamOutput) {
        TODO("Not yet implemented")
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(SM_METADATA_TYPE)
            .field(POLICY_SEQ_NO, policySeqNo)
            .field(POLICY_PRIMARY_TERM, policyPrimaryTerm)
            .field(CURRENT_STATE, currentState)
            .field(API_CALLING, apiCalling)
            .optionalTimeField(NEXT_CREATION_TIME, nextCreationTime)
            .field(CREATING, creating)
            .optionalTimeField(NEXT_DELETION_TIME, nextDeletionTime)
            .field(DELETING, deleting)
            .field(INFO, info)
            .endObject()
            .endObject()
    }

    companion object {
        const val SM_METADATA_TYPE = "sm_metadata"
        const val POLICY_SEQ_NO = "policy_seq_no"
        const val POLICY_PRIMARY_TERM = "policy_primary_term"
        const val CURRENT_STATE = "current_state"
        const val API_CALLING = "api_calling"
        const val NEXT_CREATION_TIME = "next_creation_time"
        const val CREATING = "creating"
        const val NEXT_DELETION_TIME = "next_deletion_time"
        const val DELETING = "deleting"
        const val INFO = "info"

        private fun <T> XContentParser.nullWrapper(block: XContentParser.() -> T): T? {
            return if (currentToken() == Token.VALUE_NULL) null else block()
        }

        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): SMMetadata {
            var policySeqNo: Long? = null
            var policyPrimaryTerm: Long? = null
            var currentState: String? = null
            var apiCalling = false
            var nextCreationTime: Instant? = null
            var creating: String? = null
            var nextDeletionTime: Instant? = null
            var deleting: List<String>? = null
            var info: Map<String, Any>? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    POLICY_SEQ_NO -> policySeqNo = xcp.longValue()
                    POLICY_PRIMARY_TERM -> policyPrimaryTerm = xcp.longValue()
                    CURRENT_STATE -> currentState = xcp.text()
                    API_CALLING -> apiCalling = xcp.booleanValue()
                    NEXT_CREATION_TIME -> nextCreationTime = xcp.instant()
                    CREATING -> creating = xcp.nullWrapper { text() }
                    NEXT_DELETION_TIME -> nextDeletionTime = xcp.instant()
                    DELETING -> deleting = xcp.parseArray { text() }
                    INFO -> info = xcp.nullWrapper { xcp.map() }
                }
            }

            return SMMetadata(
                policySeqNo = requireNotNull(policySeqNo) {},
                policyPrimaryTerm = requireNotNull(policyPrimaryTerm) {},
                currentState = requireNotNull(currentState) {},
                apiCalling = apiCalling,
                nextCreationTime = nextCreationTime,
                creating = creating,
                nextDeletionTime = nextDeletionTime,
                deleting = deleting,
                info = info,
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm
            )
        }

        fun InfoType.upsert(key: String, value: String): InfoType {
            if (this == null) return this
            val info = this.toMutableMap()
            info[key] = value
            return info
        }
    }
}

typealias InfoType = Map<String, Any>?
