/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.apache.logging.log4j.LogManager
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
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import org.opensearch.indexmanagement.opensearchapi.parseArray
import org.opensearch.indexmanagement.util.NO_ID
import java.time.Instant

data class SMMetadata(
    val policySeqNo: Long,
    val policyPrimaryTerm: Long,
    val currentState: String, // TODO maybe change to SMState is better
    val apiCalling: Boolean = false,
    val creation: Creation,
    val deletion: Deletion,
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

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        // TODO no need to save if the field is null
        return builder.startObject()
            .startObject(SM_METADATA_TYPE)
            .field(POLICY_SEQ_NO_FIELD, policySeqNo)
            .field(POLICY_PRIMARY_TERM_FIELD, policyPrimaryTerm)
            .field(CURRENT_STATE_FIELD, currentState)
            .field(API_CALLING_FIELD, apiCalling)
            .field(CREATION_FIELD, creation)
            .field(DELETION_FIELD, deletion)
            .optionalTimeField(NEXT_CREATION_TIME, nextCreationTime)
            .field(CREATING, creating)
            .optionalTimeField(NEXT_DELETION_TIME, nextDeletionTime)
            .field(DELETING, deleting)
            .field(INFO_FIELD, info)
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
        const val NEXT_CREATION_TIME = "next_creation_time"
        const val CREATING = "creating"
        const val NEXT_DELETION_TIME = "next_deletion_time"
        const val DELETING = "deleting"
        const val INFO_FIELD = "info"

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
            var creation: Creation? = null
            var deletion: Deletion? = null
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
                    POLICY_SEQ_NO_FIELD -> policySeqNo = xcp.longValue()
                    POLICY_PRIMARY_TERM_FIELD -> policyPrimaryTerm = xcp.longValue()
                    CURRENT_STATE_FIELD -> currentState = xcp.text()
                    API_CALLING_FIELD -> apiCalling = xcp.booleanValue()
                    CREATION_FIELD -> creation = Creation.parse(xcp)
                    DELETION_FIELD -> deletion = Deletion.parse(xcp)
                    NEXT_CREATION_TIME -> nextCreationTime = xcp.instant()
                    CREATING -> creating = xcp.nullWrapper { text() }
                    NEXT_DELETION_TIME -> nextDeletionTime = xcp.instant()
                    DELETING -> deleting = xcp.parseArray { text() }
                    INFO_FIELD -> info = xcp.nullWrapper { xcp.map() }
                }
            }

            return SMMetadata(
                policySeqNo = requireNotNull(policySeqNo) {},
                policyPrimaryTerm = requireNotNull(policyPrimaryTerm) {},
                currentState = requireNotNull(currentState) {},
                apiCalling = apiCalling,
                creation = requireNotNull(creation) {}, // won't be null from start
                deletion = requireNotNull(deletion) {},
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

    // constructor(sin: StreamInput): this(
    //
    // )

    override fun writeTo(out: StreamOutput) {
    }

    data class Creation(
        val trigger: Trigger,
        val started: String? = null,
        val finished: String? = null,
    ) : Writeable, ToXContentFragment {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(TRIGGER_FIELD, trigger)
                .field(STARTED_FIELD, started)
                .field(FINISHED_FIELD, finished)
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
                        STARTED_FIELD -> started = xcp.text()
                        FINISHED_FIELD -> finished = xcp.text()
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
            started = sin.readString(),
            finished = sin.readString(),
        )

        override fun writeTo(out: StreamOutput) {
            trigger.writeTo(out)
            out.writeString(started)
            out.writeString(finished)
        }
    }

    data class Deletion(
        val trigger: Trigger,
        val started: List<String>? = null,
    ) : Writeable, ToXContentFragment {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(TRIGGER_FIELD, trigger)
                .field(STARTED_FIELD, started)
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
                        STARTED_FIELD -> started = xcp.parseArray { text() }
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
            started = sin.readStringList()
        )

        override fun writeTo(out: StreamOutput) {
            trigger.writeTo(out)
            out.writeStringArray(started?.toTypedArray())
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

typealias InfoType = Map<String, Any>?
