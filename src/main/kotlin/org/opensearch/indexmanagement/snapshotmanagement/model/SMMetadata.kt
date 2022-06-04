/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.opensearchapi.instant
import org.opensearch.indexmanagement.opensearchapi.nullValueHandler
import org.opensearch.indexmanagement.opensearchapi.optionalField
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import org.opensearch.indexmanagement.opensearchapi.parseArray
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Retry.Companion.RETRY_FIELD
import org.opensearch.indexmanagement.util.NO_ID
import java.time.Instant

typealias InfoType = Map<String, Any>

data class SMMetadata(
    val policySeqNo: Long,
    val policyPrimaryTerm: Long,
    val currentState: SMState,
    val creation: WorkflowMetadata,
    val deletion: WorkflowMetadata,
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
) : Writeable, ToXContentObject {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(SM_METADATA_TYPE)
            .field(POLICY_SEQ_NO_FIELD, policySeqNo)
            .field(POLICY_PRIMARY_TERM_FIELD, policyPrimaryTerm)
            .field(CURRENT_STATE_FIELD, currentState.toString())
            .field(CREATION_FIELD, creation)
            .field(DELETION_FIELD, deletion)
            .endObject()
            .endObject()
    }

    companion object {
        const val SM_METADATA_TYPE = "sm_metadata"
        const val POLICY_SEQ_NO_FIELD = "policy_seq_no"
        const val POLICY_PRIMARY_TERM_FIELD = "policy_primary_term"
        const val CURRENT_STATE_FIELD = "current_state"
        const val CREATION_FIELD = "creation"
        const val DELETION_FIELD = "deletion"

        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): SMMetadata {
            var policySeqNo: Long? = null
            var policyPrimaryTerm: Long? = null
            var currentState: SMState? = null
            var creation: WorkflowMetadata? = null
            var deletion: WorkflowMetadata? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    POLICY_SEQ_NO_FIELD -> policySeqNo = xcp.longValue()
                    POLICY_PRIMARY_TERM_FIELD -> policyPrimaryTerm = xcp.longValue()
                    CURRENT_STATE_FIELD -> currentState = SMState.valueOf(xcp.text())
                    CREATION_FIELD -> creation = WorkflowMetadata.parse(xcp)
                    DELETION_FIELD -> deletion = WorkflowMetadata.parse(xcp)
                }
            }

            return SMMetadata(
                policySeqNo = requireNotNull(policySeqNo) { "policy_seq_no field must not be null" },
                policyPrimaryTerm = requireNotNull(policyPrimaryTerm) { "policy_primary_term field must not be null" },
                currentState = requireNotNull(currentState) { "current_state field must not be null" },
                creation = requireNotNull(creation) { "creation field must not be null" },
                deletion = requireNotNull(deletion) { "deletion field must not be null" },
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm
            )
        }

        fun InfoType?.upsert(keyValuePair: Pair<String, String>): InfoType {
            val info: MutableMap<String, Any> = this?.toMutableMap() ?: mutableMapOf()
            info[keyValuePair.first] = keyValuePair.second
            return info
        }

        fun InfoType?.remove(key: String): InfoType? {
            return this?.toMutableMap().remove(key)
        }
    }

    constructor(sin: StreamInput) : this(
        policySeqNo = sin.readLong(),
        policyPrimaryTerm = sin.readLong(),
        currentState = sin.readEnum(SMState::class.java),
        creation = WorkflowMetadata(sin),
        deletion = WorkflowMetadata(sin),
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeLong(policySeqNo)
        out.writeLong(policyPrimaryTerm)
        out.writeEnum(currentState)
        creation.writeTo(out)
        deletion.writeTo(out)
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
    }

    data class WorkflowMetadata(
        val trigger: Trigger,
        val started: List<String>? = null,
        val latestExecution: LatestExecution? = null,
        val retry: Retry? = null,
    ) : Writeable, ToXContentObject {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(TRIGGER_FIELD, trigger)
                .optionalField(STARTED_FIELD, started)
                .optionalField(LAST_EXECUTION_FIELD, latestExecution)
                .optionalField(RETRY_FIELD, retry)
                .endObject()
        }

        companion object {
            const val TRIGGER_FIELD = "trigger"
            const val STARTED_FIELD = "started"
            const val LAST_EXECUTION_FIELD = "latest_execution"

            fun parse(xcp: XContentParser): WorkflowMetadata {
                var trigger: Trigger? = null
                var started: List<String>? = null
                var latestExecution: LatestExecution? = null
                var retry: Retry? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        TRIGGER_FIELD -> trigger = Trigger.parse(xcp)
                        STARTED_FIELD -> started = xcp.nullValueHandler { parseArray { text() } }
                        LAST_EXECUTION_FIELD -> latestExecution = xcp.nullValueHandler { LatestExecution.parse(xcp) }
                        RETRY_FIELD -> retry = xcp.nullValueHandler { Retry.parse(xcp) }
                    }
                }

                return WorkflowMetadata(
                    trigger = requireNotNull(trigger) { "trigger field must not be null." },
                    started = started,
                    latestExecution = latestExecution,
                    retry = retry,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            trigger = Trigger(sin),
            started = sin.readOptionalStringList(),
            latestExecution = sin.readOptionalWriteable { LatestExecution(it) },
            retry = sin.readOptionalWriteable { Retry(it) },
        )

        override fun writeTo(out: StreamOutput) {
            trigger.writeTo(out)
            out.writeOptionalStringCollection(started)
            out.writeOptionalWriteable(latestExecution)
            out.writeOptionalWriteable(retry)
        }
    }

    data class LatestExecution(
        val status: Status,
        val startTime: Instant,
        val endTime: Instant? = null,
        val message: String? = null,
        val snapshot: List<String>? = null,
    ) : Writeable, ToXContentObject {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(STATUS_FIELD, status.toString())
                .optionalTimeField(START_TIME_FIELD, startTime)
                .optionalTimeField(END_TIME_FIELD, endTime)
                .optionalField(MESSAGE_FIELD, message)
                .optionalField(SNAPSHOT_FIELD, snapshot)
                .endObject()
        }

        companion object {
            const val STATUS_FIELD = "status"
            const val START_TIME_FIELD = "start_time"
            const val END_TIME_FIELD = "end_time"
            const val MESSAGE_FIELD = "message"
            const val SNAPSHOT_FIELD = "snapshot"

            fun parse(xcp: XContentParser): LatestExecution {
                var status: Status? = null
                var startTime: Instant? = null
                var endTime: Instant? = null
                var message: String? = null
                var snapshot: List<String>? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        STATUS_FIELD -> status = Status.valueOf(xcp.text())
                        START_TIME_FIELD -> startTime = xcp.instant()
                        END_TIME_FIELD -> endTime = xcp.instant()
                        MESSAGE_FIELD -> message = xcp.nullValueHandler { text() }
                        SNAPSHOT_FIELD -> snapshot = xcp.nullValueHandler { parseArray { text() } }
                    }
                }

                return LatestExecution(
                    status = requireNotNull(status) { "last_execution.status must not be null" },
                    startTime = requireNotNull(startTime) { "last_execution.start_time must not be null" },
                    endTime = endTime,
                    message = message,
                    snapshot = snapshot,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            status = sin.readEnum(Status::class.java),
            startTime = sin.readInstant(),
            endTime = sin.readOptionalInstant(),
            message = sin.readOptionalString(),
            snapshot = sin.readOptionalStringList(),
        )

        override fun writeTo(out: StreamOutput) {
            out.writeEnum(status)
            out.writeInstant(startTime)
            out.writeOptionalInstant(endTime)
            out.writeOptionalString(message)
            out.writeOptionalStringCollection(snapshot)
        }

        enum class Status {
            IN_PROGRESS,
            SUCCESS,
            RETRYING,
            FAILED,
            TIME_LIMIT_EXCEEDED,
        }
    }

    /**
     * Trigger for recurring condition check
     *
     * index_size can be another possible trigger, e.g.: snapshot will be created
     * every time index size increases 50gb
     */
    data class Trigger(
        val time: Instant,
    ) : Writeable, ToXContentObject {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .optionalTimeField(TIME_FIELD, time)
                .endObject()
        }

        companion object {
            const val TIME_FIELD = "time"

            fun parse(xcp: XContentParser): Trigger {
                var nextExecutionTime: Instant? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        TIME_FIELD -> nextExecutionTime = xcp.instant()
                    }
                }

                return Trigger(
                    time = requireNotNull(nextExecutionTime) { "trigger time field must not be null." },
                )
            }
        }

        constructor(sin: StreamInput) : this(
            time = sin.readInstant()
        )

        override fun writeTo(out: StreamOutput) {
            out.writeInstant(time)
        }
    }

    data class Retry(
        val count: Int,
    ) : Writeable, ToXContentObject {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(COUNT_FIELD, count)
                .endObject()
        }

        companion object {
            const val RETRY_FIELD = "retry"
            const val COUNT_FIELD = "count"

            fun parse(xcp: XContentParser): Retry {
                var count: Int? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        COUNT_FIELD -> count = xcp.intValue()
                    }
                }

                return Retry(
                    count = requireNotNull(count) { "count field in Retry must not be null." }
                )
            }
        }

        constructor(sin: StreamInput) : this(
            count = sin.readInt()
        )

        override fun writeTo(out: StreamOutput) {
            out.writeInt(count)
        }
    }

    /**
     * Build the updated metadata in a flattened fashion
     *  based on the existing metadata
     */
    class Builder(private var metadata: SMMetadata) {

        fun build() = metadata

        // Reset the related metadata fields of the workflow
        // so the state machine will skip/abort this execution
        // and can go to the next execution period
        fun reset(workflowType: WorkflowType): Builder {
            var currentState = metadata.currentState
            var startedCreation = metadata.creation.started
            var creationRetry = metadata.creation.retry
            var startedDeletion = metadata.deletion.started
            var deletionRetry = metadata.deletion.retry
            when (workflowType) {
                WorkflowType.CREATION -> {
                    currentState = SMState.CREATING
                    startedCreation = null
                    creationRetry = null
                }
                WorkflowType.DELETION -> {
                    currentState = SMState.DELETING
                    startedDeletion = null
                    deletionRetry = null
                }
            }

            metadata = metadata.copy(
                currentState = currentState,
                creation = metadata.creation.copy(
                    started = startedCreation,
                    retry = creationRetry,
                ),
                deletion = metadata.deletion.copy(
                    started = startedDeletion,
                    retry = deletionRetry,
                ),
            )
            return this
        }

        fun setRetry(workflowType: WorkflowType, count: Int): Builder {
            when (workflowType) {
                WorkflowType.CREATION -> {
                    metadata = metadata.copy(
                        creation = metadata.creation.copy(
                            retry = Retry(count = count)
                        )
                    )
                }
                WorkflowType.DELETION -> {
                    metadata = metadata.copy(
                        deletion = metadata.deletion.copy(
                            retry = Retry(count = count)
                        )
                    )
                }
            }
            return this
        }

        fun resetRetry(creation: Boolean = false, deletion: Boolean = false): Builder {
            if (creation && metadata.creation.retry != null) {
                metadata = metadata.copy(
                    creation = metadata.creation.copy(
                        retry = null
                    )
                )
            }
            if (deletion && metadata.deletion.retry != null) {
                metadata = metadata.copy(
                    deletion = metadata.deletion.copy(
                        retry = null
                    )
                )
            }
            return this
        }

        fun policyVersion(seqNo: Long, primaryTerm: Long): Builder {
            metadata = metadata.copy(
                policySeqNo = seqNo,
                policyPrimaryTerm = primaryTerm,
            )
            return this
        }

        fun currentState(state: SMState): Builder {
            metadata = metadata.copy(
                currentState = state
            )
            return this
        }

        fun nextCreationTime(time: Instant): Builder {
            metadata = metadata.copy(
                creation = metadata.creation.copy(
                    trigger = metadata.creation.trigger.copy(
                        time = time
                    )
                )
            )
            return this
        }

        // TODO SM update execution metadata and init execution metadata
        fun creation(snapshot: String?, execution: LatestExecution): Builder {
            metadata = metadata.copy(
                creation = metadata.creation.copy(
                    started = if (snapshot == null) null else listOf(snapshot),
                    latestExecution = execution,
                )
            )
            return this
        }

        fun nextDeletionTime(time: Instant): Builder {
            metadata = metadata.copy(
                deletion = metadata.deletion.copy(
                    trigger = metadata.deletion.trigger.copy(
                        time = time
                    )
                )
            )
            return this
        }

        fun deletion(snapshots: List<String>?, execution: LatestExecution): Builder {
            metadata = metadata.copy(
                deletion = metadata.deletion.copy(
                    started = snapshots,
                    latestExecution = execution,
                )
            )
            return this
        }
    }
}
