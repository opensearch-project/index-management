/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
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
import org.opensearch.indexmanagement.opensearchapi.readOptionalValue
import org.opensearch.indexmanagement.opensearchapi.writeOptionalValue
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
    val creation: Creation,
    val deletion: Deletion,
    val info: InfoType? = null,
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
) : Writeable, ToXContent {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(SM_METADATA_TYPE)
            .field(POLICY_SEQ_NO_FIELD, policySeqNo)
            .field(POLICY_PRIMARY_TERM_FIELD, policyPrimaryTerm)
            .field(CURRENT_STATE_FIELD, currentState.toString())
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
            var creation: Creation? = null
            var deletion: Deletion? = null
            var info: InfoType? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    POLICY_SEQ_NO_FIELD -> policySeqNo = xcp.longValue()
                    POLICY_PRIMARY_TERM_FIELD -> policyPrimaryTerm = xcp.longValue()
                    CURRENT_STATE_FIELD -> currentState = SMState.valueOf(xcp.text())
                    CREATION_FIELD -> creation = Creation.parse(xcp)
                    DELETION_FIELD -> deletion = Deletion.parse(xcp)
                    INFO_FIELD -> info = xcp.nullValueHandler { xcp.map() }
                }
            }

            return SMMetadata(
                policySeqNo = requireNotNull(policySeqNo) {},
                policyPrimaryTerm = requireNotNull(policyPrimaryTerm) {},
                currentState = requireNotNull(currentState) {},
                creation = requireNotNull(creation) {},
                deletion = requireNotNull(deletion) {},
                info = info,
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
        creation.writeTo(out)
        deletion.writeTo(out)
        out.writeMap(info)
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
    }

    data class Creation(
        val trigger: Trigger,
        val started: SnapshotInfo? = null,
        val finished: SnapshotInfo? = null,
        val retry: Retry? = null,
    ) : Writeable, ToXContent {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(TRIGGER_FIELD, trigger)
                .optionalField(STARTED_FIELD, started)
                .optionalField(FINISHED_FIELD, finished)
                .optionalField(RETRY_FIELD, retry)
                .endObject()
        }

        companion object {
            const val TRIGGER_FIELD = "trigger"
            const val STARTED_FIELD = "started"
            const val FINISHED_FIELD = "finished"

            fun parse(xcp: XContentParser): Creation {
                var trigger: Trigger? = null
                var started: SnapshotInfo? = null
                var finished: SnapshotInfo? = null
                var retry: Retry? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        TRIGGER_FIELD -> trigger = Trigger.parse(xcp)
                        STARTED_FIELD -> started = xcp.nullValueHandler { SnapshotInfo.parse(xcp) }
                        FINISHED_FIELD -> finished = xcp.nullValueHandler { SnapshotInfo.parse(xcp) }
                        RETRY_FIELD -> retry = xcp.nullValueHandler { Retry.parse(xcp) }
                    }
                }

                return Creation(
                    trigger = requireNotNull(trigger) { "trigger field must not be null." },
                    started = started,
                    finished = finished,
                    retry = retry,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            trigger = Trigger(sin),
            started = sin.readOptionalWriteable { SnapshotInfo(it) },
            finished = sin.readOptionalWriteable { SnapshotInfo(it) },
            retry = sin.readOptionalWriteable { Retry(it) },
        )

        override fun writeTo(out: StreamOutput) {
            trigger.writeTo(out)
            out.writeOptionalWriteable(started)
            out.writeOptionalWriteable(finished)
            out.writeOptionalWriteable(retry)
        }
    }

    data class Retry(
        val count: Int,
    ) : Writeable, ToXContent {

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

    data class Deletion(
        val trigger: Trigger,
        val started: List<SnapshotInfo>? = null,
        val startedTime: Instant? = null,
        val retry: Retry? = null,
    ) : Writeable, ToXContent {

        init {
            require(!(started != null).xor(startedTime != null)) {
                "deletion started and startedTime must exist at the same time."
            }
        }

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(TRIGGER_FIELD, trigger)
                .optionalField(STARTED_FIELD, started)
                .optionalField(STARTED_TIME_FIELD, startedTime)
                .optionalTimeField(STARTED_TIME_FIELD, startedTime)
                .optionalField(RETRY_FIELD, retry)
                .endObject()
        }

        companion object {
            const val TRIGGER_FIELD = "trigger"
            const val STARTED_FIELD = "started"
            const val STARTED_TIME_FIELD = "started_time"

            fun parse(xcp: XContentParser): Deletion {
                var trigger: Trigger? = null
                var started: List<SnapshotInfo>? = null
                var startedTime: Instant? = null
                var retry: Retry? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        TRIGGER_FIELD -> trigger = Trigger.parse(xcp)
                        STARTED_FIELD -> started = xcp.nullValueHandler { parseArray { SnapshotInfo.parse(xcp) } }
                        STARTED_TIME_FIELD -> startedTime = xcp.instant()
                        RETRY_FIELD -> retry = xcp.nullValueHandler { Retry.parse(xcp) }
                    }
                }

                return Deletion(
                    trigger = requireNotNull(trigger) { "trigger field must not be null" },
                    started = started,
                    startedTime = startedTime,
                    retry = retry,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            trigger = Trigger(sin),
            started = sin.readOptionalValue(sin.readList { SnapshotInfo(it) }),
            startedTime = sin.readOptionalInstant(),
            retry = sin.readOptionalWriteable { Retry(it) },
        )

        override fun writeTo(out: StreamOutput) {
            trigger.writeTo(out)
            out.writeOptionalValue(started, StreamOutput::writeList)
            out.writeOptionalInstant(startedTime)
            out.writeOptionalWriteable(retry)
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
    ) : Writeable, ToXContent {

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

    data class SnapshotInfo(
        val name: String,
        val startTime: Instant,
        val endTime: Instant? = null,
    ) : Writeable, ToXContent {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(NAME_FIELD, name)
                .optionalTimeField(START_TIME_FIELD, startTime)
                .optionalTimeField(END_TIME_FIELD, endTime)
                .endObject()
        }

        companion object {
            const val NAME_FIELD = "name"
            const val START_TIME_FIELD = "start_time"
            const val END_TIME_FIELD = "end_time"

            fun parse(xcp: XContentParser): SnapshotInfo {
                var name: String? = null
                var startTime: Instant? = null
                var endTime: Instant? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        NAME_FIELD -> name = xcp.text()
                        START_TIME_FIELD -> startTime = xcp.instant()
                        END_TIME_FIELD -> endTime = xcp.instant()
                    }
                }

                return SnapshotInfo(
                    name = requireNotNull(name) { "name in snapshot info must not be null." },
                    startTime = requireNotNull(startTime) { "start time in snapshot info must not be null." },
                    endTime = endTime,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            name = sin.readString(),
            startTime = sin.readInstant(),
            endTime = sin.readOptionalInstant(),
        )

        override fun writeTo(out: StreamOutput) {
            out.writeString(name)
            out.writeInstant(startTime)
            out.writeOptionalInstant(endTime)
        }
    }

    /**
     * Build the updated metadata in a flattened fashion
     *  based on the existing metadata
     */
    class Builder(private var metadata: SMMetadata) {

        fun build() = metadata

        // Reset the workflow
        fun reset(workflowType: WorkflowType): Builder {
            var currentState = metadata.currentState
            var startedCreation = metadata.creation.started
            var creationRetry = metadata.creation.retry
            var startedDeletion = metadata.deletion.started
            var deletionStartedTime = metadata.deletion.startedTime
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
                    deletionStartedTime = null
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
                    startedTime = deletionStartedTime,
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

        fun creation(snapshotInfo: SnapshotInfo?): Builder {
            metadata = metadata.copy(
                creation = metadata.creation.copy(
                    started = snapshotInfo
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

        fun deletion(startTime: Instant?, snapshotInfo: List<SnapshotInfo>?): Builder {
            metadata = metadata.copy(
                deletion = metadata.deletion.copy(
                    started = snapshotInfo,
                    startedTime = startTime,
                )
            )
            return this
        }

        fun info(info: InfoType?): Builder {
            metadata = metadata.copy(
                info = info
            )
            return this
        }
    }
}
