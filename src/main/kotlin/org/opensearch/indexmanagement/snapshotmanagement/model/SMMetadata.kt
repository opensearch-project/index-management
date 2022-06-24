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
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.opensearchapi.instant
import org.opensearch.indexmanagement.opensearchapi.nullValueHandler
import org.opensearch.indexmanagement.opensearchapi.optionalField
import org.opensearch.indexmanagement.opensearchapi.optionalInfoField
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import org.opensearch.indexmanagement.opensearchapi.parseArray
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementException
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Retry.Companion.RETRY_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.NAME_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.preFixTimeStamp
import org.opensearch.indexmanagement.snapshotmanagement.smMetadataDocIdToPolicyName
import org.opensearch.indexmanagement.util.NO_ID
import java.time.Instant
import java.time.Instant.now

typealias InfoType = Map<String, Any>

data class SMMetadata(
    val policySeqNo: Long,
    val policyPrimaryTerm: Long,
    val creation: WorkflowMetadata,
    val deletion: WorkflowMetadata?,
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
) : Writeable, ToXContentObject {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(SM_METADATA_TYPE)
        builder.field(NAME_FIELD, smMetadataDocIdToPolicyName(id))
            .field(POLICY_SEQ_NO_FIELD, policySeqNo)
            .field(POLICY_PRIMARY_TERM_FIELD, policyPrimaryTerm)
            .field(CREATION_FIELD, creation)
            .optionalField(DELETION_FIELD, deletion)
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        return builder.endObject()
    }

    companion object {
        const val SM_METADATA_TYPE = "sm_metadata"
        const val POLICY_SEQ_NO_FIELD = "policy_seq_no"
        const val POLICY_PRIMARY_TERM_FIELD = "policy_primary_term"
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
            var creation: WorkflowMetadata? = null
            var deletion: WorkflowMetadata? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME_FIELD -> requireNotNull(xcp.text()) { "The name field of SMPolicy must not be null." }
                    POLICY_SEQ_NO_FIELD -> policySeqNo = xcp.longValue()
                    POLICY_PRIMARY_TERM_FIELD -> policyPrimaryTerm = xcp.longValue()
                    CREATION_FIELD -> creation = WorkflowMetadata.parse(xcp)
                    DELETION_FIELD -> deletion = WorkflowMetadata.parse(xcp)
                }
            }

            return SMMetadata(
                policySeqNo = requireNotNull(policySeqNo) { "policy_seq_no field must not be null" },
                policyPrimaryTerm = requireNotNull(policyPrimaryTerm) { "policy_primary_term field must not be null" },
                creation = requireNotNull(creation) { "creation field must not be null" },
                deletion = deletion,
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
        creation = WorkflowMetadata(sin),
        deletion = sin.readOptionalWriteable { WorkflowMetadata(it) },
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeLong(policySeqNo)
        out.writeLong(policyPrimaryTerm)
        creation.writeTo(out)
        out.writeOptionalWriteable(deletion)
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
    }

    data class WorkflowMetadata(
        val currentState: SMState,
        val trigger: Trigger,
        val started: List<String>? = null,
        val latestExecution: LatestExecution? = null,
        val retry: Retry? = null,
    ) : Writeable, ToXContentObject {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(CURRENT_STATE_FIELD, currentState.toString())
                .field(TRIGGER_FIELD, trigger)
                .optionalField(STARTED_FIELD, started)
                .optionalField(LAST_EXECUTION_FIELD, latestExecution)
                .optionalField(RETRY_FIELD, retry)
                .endObject()
        }

        companion object {
            const val CURRENT_STATE_FIELD = "current_state"
            const val TRIGGER_FIELD = "trigger"
            const val STARTED_FIELD = "started"
            const val LAST_EXECUTION_FIELD = "latest_execution"

            fun parse(xcp: XContentParser): WorkflowMetadata {
                var currentState: SMState? = null
                var trigger: Trigger? = null
                var started: List<String>? = null
                var latestExecution: LatestExecution? = null
                var retry: Retry? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        CURRENT_STATE_FIELD -> currentState = SMState.valueOf(xcp.text())
                        TRIGGER_FIELD -> trigger = Trigger.parse(xcp)
                        STARTED_FIELD -> started = xcp.nullValueHandler { parseArray { text() } }
                        LAST_EXECUTION_FIELD -> latestExecution = xcp.nullValueHandler { LatestExecution.parse(xcp) }
                        RETRY_FIELD -> retry = xcp.nullValueHandler { Retry.parse(xcp) }
                    }
                }

                return WorkflowMetadata(
                    currentState = requireNotNull(currentState) { "current_state field must not be null" },
                    trigger = requireNotNull(trigger) { "trigger field must not be null." },
                    started = started,
                    latestExecution = latestExecution,
                    retry = retry,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            currentState = sin.readEnum(SMState::class.java),
            trigger = Trigger(sin),
            started = sin.readOptionalStringList(),
            latestExecution = sin.readOptionalWriteable { LatestExecution(it) },
            retry = sin.readOptionalWriteable { Retry(it) },
        )

        override fun writeTo(out: StreamOutput) {
            out.writeEnum(currentState)
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
        val info: Info? = null,
    ) : Writeable, ToXContentObject {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(STATUS_FIELD, status.toString())
                .optionalTimeField(START_TIME_FIELD, startTime)
                .optionalTimeField(END_TIME_FIELD, endTime)
                .optionalInfoField(INFO_FIELD, info)
                .endObject()
        }

        companion object {
            const val STATUS_FIELD = "status"
            const val START_TIME_FIELD = "start_time"
            const val END_TIME_FIELD = "end_time"
            const val INFO_FIELD = "info"

            fun parse(xcp: XContentParser): LatestExecution {
                var status: Status? = null
                var startTime: Instant? = null
                var endTime: Instant? = null
                var info: Info? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        STATUS_FIELD -> status = Status.valueOf(xcp.text())
                        START_TIME_FIELD -> startTime = xcp.instant()
                        END_TIME_FIELD -> endTime = xcp.instant()
                        INFO_FIELD -> info = Info.parse(xcp)
                    }
                }

                return LatestExecution(
                    status = requireNotNull(status) { "last_execution.status must not be null" },
                    startTime = requireNotNull(startTime) { "last_execution.start_time must not be null" },
                    endTime = endTime,
                    info = info,
                )
            }

            fun init(status: Status, info: Info? = null): LatestExecution {
                return LatestExecution(
                    status = status,
                    startTime = now(),
                    info = info,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            status = sin.readEnum(Status::class.java),
            startTime = sin.readInstant(),
            endTime = sin.readOptionalInstant(),
            info = sin.readOptionalWriteable { Info(it) },
        )

        override fun writeTo(out: StreamOutput) {
            out.writeEnum(status)
            out.writeInstant(startTime)
            out.writeOptionalInstant(endTime)
            out.writeOptionalWriteable(info)
        }

        enum class Status {
            IN_PROGRESS,
            RETRYING,
            SUCCESS,
            FAILED,
            TIME_LIMIT_EXCEEDED,
        }
    }

    data class Info(
        val message: String? = null,
        val cause: String? = null,
    ) : Writeable, ToXContentObject {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .optionalField(MESSAGE_FIELD, message)
                .optionalField(CAUSE_FIELD, cause)
                .endObject()
        }

        companion object {
            const val MESSAGE_FIELD = "message"
            const val CAUSE_FIELD = "cause"

            fun parse(xcp: XContentParser): Info {
                var message: String? = null
                var cause: String? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        MESSAGE_FIELD -> message = xcp.text()
                        CAUSE_FIELD -> cause = xcp.text()
                    }
                }

                return Info(
                    message = message,
                    cause = cause,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            message = sin.readOptionalString(),
            cause = sin.readOptionalString(),
        )

        override fun writeTo(out: StreamOutput) {
            out.writeOptionalString(message)
            out.writeOptionalString(cause)
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
    @Suppress("TooManyFunctions")
    class Builder(
        private var metadata: SMMetadata,
    ) {

        fun build() = metadata

        private lateinit var workflowType: WorkflowType
        fun workflow(workflowType: WorkflowType): Builder {
            this.workflowType = workflowType
            return this
        }

        fun setCurrentState(state: SMState): Builder {
            when (workflowType) {
                WorkflowType.CREATION -> {
                    metadata = metadata.copy(
                        creation = metadata.creation.copy(
                            currentState = state
                        )
                    )
                }
                WorkflowType.DELETION -> {
                    metadata = metadata.copy(
                        deletion = metadata.deletion?.copy(
                            currentState = state
                        )
                    )
                }
            }
            return this
        }

        // Reset the workflow of this execution period so SM can
        // go to execute the next
        fun resetWorkflow(): Builder {
            var creationCurrentState = metadata.creation.currentState
            var startedCreation = metadata.creation.started
            var creationRetry = metadata.creation.retry
            var deletionCurrentState = metadata.deletion?.currentState
            var startedDeletion = metadata.deletion?.started
            var deletionRetry = metadata.deletion?.retry
            when (workflowType) {
                WorkflowType.CREATION -> {
                    creationCurrentState = SMState.CREATION_START
                    startedCreation = null
                    creationRetry = null
                }
                WorkflowType.DELETION -> {
                    deletionCurrentState = SMState.DELETION_START
                    startedDeletion = null
                    deletionRetry = null
                }
            }

            metadata = metadata.copy(
                creation = metadata.creation.copy(
                    currentState = creationCurrentState,
                    started = startedCreation,
                    retry = creationRetry,
                ),
                deletion = deletionCurrentState?.let {
                    metadata.deletion?.copy(
                        currentState = it,
                        started = startedDeletion,
                        retry = deletionRetry,
                    )
                },
            )
            return this
        }

        fun resetDeletion(): Builder {
            metadata = metadata.copy(
                deletion = null
            )
            return this
        }

        // Use this **first** to update metadata, because it depends on started field
        // So if you change started first, this could behave wrongly
        @Suppress("LongParameterList")
        fun setLatestExecution(
            status: LatestExecution.Status,
            updateMessage: Boolean = true,
            message: String? = null,
            updateCause: Boolean = true,
            cause: Exception? = null,
            endTime: Instant? = null
        ): Builder {
            val messageWithTime = if (message != null) preFixTimeStamp(message) else null
            val causeWithTime = if (cause != null) preFixTimeStamp(SnapshotManagementException.getUserErrorMessage(cause).message) else null
            fun getUpdatedWorkflowMetadata(workflowMetadata: WorkflowMetadata): WorkflowMetadata {
                // if started is null, we need to override the previous latestExecution
                //  w/ a newly initialized one
                if (workflowMetadata.started == null) {
                    return workflowMetadata.copy(
                        latestExecution = LatestExecution.init(
                            status = status,
                            info = Info(
                                message = messageWithTime,
                                cause = causeWithTime,
                            )
                        )
                    )
                } else {
                    // if started is not null, latestExecution should never be null
                    return workflowMetadata.copy(
                        latestExecution = workflowMetadata.latestExecution?.copy(
                            status = status,
                            info = Info(
                                message = if (updateMessage) messageWithTime else metadata.creation.latestExecution?.info?.message,
                                cause = if (updateCause) causeWithTime else metadata.creation.latestExecution?.info?.cause,
                            ),
                            endTime = endTime,
                        )
                    )
                }
            }
            metadata = when (workflowType) {
                WorkflowType.CREATION -> {
                    metadata.copy(
                        creation = getUpdatedWorkflowMetadata(metadata.creation)
                    )
                }
                WorkflowType.DELETION -> {
                    metadata.copy(
                        deletion = metadata.deletion?.let { getUpdatedWorkflowMetadata(it) }
                    )
                }
            }
            return this
        }

        fun getWorkflowMetadata(): WorkflowMetadata? {
            return when (workflowType) {
                WorkflowType.CREATION -> {
                    metadata.creation
                }
                WorkflowType.DELETION -> {
                    metadata.deletion
                }
            }
        }

        fun getWorkflowType(): WorkflowType {
            return workflowType
        }

        fun getStartedSnapshots(): List<String>? {
            return when (workflowType) {
                WorkflowType.CREATION -> {
                    metadata.creation.started
                }
                WorkflowType.DELETION -> {
                    metadata.deletion?.started
                }
            }
        }

        fun setRetry(count: Int): Builder {
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
                        deletion = metadata.deletion?.copy(
                            retry = Retry(count = count)
                        )
                    )
                }
            }
            return this
        }

        // This should be used after every [SMResult.Fail]
        fun resetRetry(): Builder {
            when (workflowType) {
                WorkflowType.CREATION -> {
                    if (metadata.creation.retry != null) {
                        metadata = metadata.copy(
                            creation = metadata.creation.copy(
                                retry = null
                            )
                        )
                    }
                }
                WorkflowType.DELETION -> {
                    if (metadata.deletion?.retry != null) {
                        metadata = metadata.copy(
                            deletion = metadata.deletion?.copy(
                                retry = null
                            )
                        )
                    }
                }
            }
            return this
        }

        fun setSeqNoPrimaryTerm(seqNo: Long, primaryTerm: Long): Builder {
            metadata = metadata.copy(
                policySeqNo = seqNo,
                policyPrimaryTerm = primaryTerm,
            )
            return this
        }

        fun setNextCreationTime(time: Instant): Builder {
            metadata = metadata.copy(
                creation = metadata.creation.copy(
                    trigger = metadata.creation.trigger.copy(
                        time = time
                    )
                )
            )
            return this
        }

        fun setCreationStarted(snapshot: String?): Builder {
            metadata = metadata.copy(
                creation = metadata.creation.copy(
                    started = if (snapshot == null) null else listOf(snapshot),
                )
            )
            return this
        }

        fun setNextDeletionTime(time: Instant): Builder {
            val deletion = metadata.deletion
            if (deletion != null) {
                metadata = metadata.copy(
                    deletion = deletion.copy(
                        trigger = deletion.trigger.copy(
                            time = time
                        )
                    )
                )
            } else {
                metadata = metadata.copy(
                    deletion = WorkflowMetadata(
                        SMState.DELETION_START,
                        Trigger(
                            time = time
                        ),
                    )
                )
            }
            return this
        }

        fun setDeletionStarted(snapshots: List<String>?): Builder {
            metadata = metadata.copy(
                deletion = metadata.deletion?.copy(
                    started = snapshots,
                )
            )
            return this
        }
    }
}
