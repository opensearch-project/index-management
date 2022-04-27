/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.opensearch.common.io.stream.StreamInput
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
import org.opensearch.indexmanagement.opensearchapi.nullParser
import org.opensearch.indexmanagement.opensearchapi.optionalField
import org.opensearch.indexmanagement.opensearchapi.parseArray
import org.opensearch.indexmanagement.opensearchapi.readOptionalType
import org.opensearch.indexmanagement.opensearchapi.writeOptionalType
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.util.NO_ID
import java.time.Instant

typealias InfoType = Map<String, Any>?

data class SMMetadata(
    val policySeqNo: Long,
    val policyPrimaryTerm: Long,
    val currentState: SMState,
    val apiCalling: Boolean = false,
    val creation: Creation,
    val deletion: Deletion,
    val info: InfoType = null,
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
) : Writeable, ToXContentFragment {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(SM_METADATA_TYPE)
            .field(POLICY_SEQ_NO_FIELD, policySeqNo)
            .field(POLICY_PRIMARY_TERM_FIELD, policyPrimaryTerm)
            .field(CURRENT_STATE_FIELD, currentState.toString())
            .field(API_CALLING_FIELD, apiCalling)
            .field(CREATION_FIELD, creation)
            .field(DELETION_FIELD, deletion)
            .optionalField(INFO_FIELD, info)
            .endObject()
            .endObject()
    }

    companion object {
        const val SM_METADATA_TYPE = "sm_metadata"
        const val POLICY_SEQ_NO_FIELD = "policy_seq_no"
        const val POLICY_PRIMARY_TERM_FIELD = "policy_primary_term"
        const val CURRENT_STATE_FIELD = "current_state"
        const val API_CALLING_FIELD = "api_calling"
        const val CREATION_FIELD = "creation"
        const val DELETION_FIELD = "deletion"
        const val INFO_FIELD = "info"

        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): SMMetadata {
            var policySeqNo: Long? = null
            var policyPrimaryTerm: Long? = null
            var currentState: SMState? = null
            var apiCalling = false
            var creation: Creation? = null
            var deletion: Deletion? = null
            var info: InfoType = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    POLICY_SEQ_NO_FIELD -> policySeqNo = xcp.longValue()
                    POLICY_PRIMARY_TERM_FIELD -> policyPrimaryTerm = xcp.longValue()
                    CURRENT_STATE_FIELD -> currentState = SMState.valueOf(xcp.text())
                    API_CALLING_FIELD -> apiCalling = xcp.booleanValue()
                    CREATION_FIELD -> creation = Creation.parse(xcp)
                    DELETION_FIELD -> deletion = Deletion.parse(xcp)
                    INFO_FIELD -> info = xcp.nullParser { xcp.map() }
                }
            }

            return SMMetadata(
                policySeqNo = requireNotNull(policySeqNo) {},
                policyPrimaryTerm = requireNotNull(policyPrimaryTerm) {},
                currentState = requireNotNull(currentState) {},
                apiCalling = apiCalling,
                creation = requireNotNull(creation) {},
                deletion = requireNotNull(deletion) {},
                info = info,
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm
            )
        }

        fun InfoType.upsert(keyValuePair: Pair<String, String>): InfoType {
            val info: MutableMap<String, Any> = this?.toMutableMap() ?: mutableMapOf()
            info[keyValuePair.first] = keyValuePair.second
            return info
        }
    }

    constructor(sin: StreamInput) : this(
        policySeqNo = sin.readLong(),
        policyPrimaryTerm = sin.readLong(),
        currentState = sin.readEnum(SMState::class.java),
        apiCalling = sin.readBoolean(),
        creation = Creation(sin),
        deletion = Deletion(sin),
        info = sin.readMap(),
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeLong(policySeqNo)
        out.writeLong(policyPrimaryTerm)
        out.writeEnum(currentState)
        out.writeBoolean(apiCalling)
        creation.writeTo(out)
        deletion.writeTo(out)
        out.writeMap(info)
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
    }

    data class Creation(
        val trigger: Trigger,
        val started: String? = null,
        val finished: String? = null,
    ) : Writeable, ToXContentFragment {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(TRIGGER_FIELD, trigger)
                .optionalField(STARTED_FIELD, started)
                .optionalField(FINISHED_FIELD, finished)
                .endObject()
        }

        companion object {
            const val TRIGGER_FIELD = "trigger"
            const val STARTED_FIELD = "started"
            const val FINISHED_FIELD = "finished"

            fun parse(xcp: XContentParser): Creation {
                var trigger: Trigger? = null
                var started: String? = null
                var finished: String? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        TRIGGER_FIELD -> trigger = Trigger.parse(xcp)
                        STARTED_FIELD -> started = xcp.nullParser { text() }
                        FINISHED_FIELD -> finished = xcp.nullParser { text() }
                    }
                }

                return Creation(
                    trigger = requireNotNull(trigger) { "trigger field must not be null" },
                    started = requireNotNull(started) { "started field must not be null" },
                    finished = requireNotNull(finished) { "finished field must not be null" },
                )
            }
        }

        constructor(sin: StreamInput) : this(
            trigger = Trigger(sin),
            started = sin.readOptionalString(),
            finished = sin.readOptionalString(),
        )

        override fun writeTo(out: StreamOutput) {
            trigger.writeTo(out)
            out.writeOptionalString(started)
            out.writeOptionalString(finished)
        }
    }

    data class Deletion(
        val trigger: Trigger,
        val started: List<String>? = null,
    ) : Writeable, ToXContentFragment {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(TRIGGER_FIELD, trigger)
                .optionalField(STARTED_FIELD, started)
                .endObject()
        }

        companion object {
            const val TRIGGER_FIELD = "trigger"
            const val STARTED_FIELD = "started"

            fun parse(xcp: XContentParser): Deletion {
                var trigger: Trigger? = null
                var started: List<String>? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        TRIGGER_FIELD -> trigger = Trigger.parse(xcp)
                        STARTED_FIELD -> started = xcp.nullParser { parseArray { text() } }
                    }
                }

                return Deletion(
                    trigger = requireNotNull(trigger) { "trigger field must not be null" },
                    started = requireNotNull(started) { "started field must not be null" },
                )
            }
        }

        constructor(sin: StreamInput) : this(
            trigger = Trigger(sin),
            started = sin.readOptionalType(StreamInput::readStringList),
        )

        override fun writeTo(out: StreamOutput) {
            trigger.writeTo(out)
            out.writeOptionalType(started?.toTypedArray(), StreamOutput::writeStringArray)
        }
    }

    data class Trigger(
        val nextExecutionTime: Instant,
    ) : Writeable, ToXContentFragment {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(NEXT_EXECUTION_TIME_FIELD, nextExecutionTime)
                .endObject()
        }

        companion object {
            const val NEXT_EXECUTION_TIME_FIELD = "next_execution_time"

            fun parse(xcp: XContentParser): Trigger {
                var nextExecutionTime: Instant? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        NEXT_EXECUTION_TIME_FIELD -> nextExecutionTime = xcp.instant()
                    }
                }

                return Trigger(
                    nextExecutionTime = requireNotNull(nextExecutionTime) { "nextExecutionTime field must not be null" },
                )
            }
        }

        constructor(sin: StreamInput) : this(
            sin.readInstant()
        )

        override fun writeTo(out: StreamOutput) {
            out.writeInstant(nextExecutionTime)
        }
    }
}
