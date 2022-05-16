/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.apache.logging.log4j.LogManager
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.unit.TimeValue
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
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.jobscheduler.spi.schedule.Schedule
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit

private val log = LogManager.getLogger(SMPolicy::class.java)

data class SMPolicy(
    val policyName: String,
    val description: String? = null,
    val creation: Creation,
    val deletion: Deletion,
    val snapshotConfig: Map<String, Any>,
    val jobEnabled: Boolean,
    val jobLastUpdateTime: Instant,
    val jobEnabledTime: Instant,
    val jobSchedule: Schedule,
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
) : ScheduledJobParameter, Writeable {

    init {
        // TODO validate snapshotConfig
        require(snapshotConfig["repository"] != null) { "Must provide a repository." }
        // indices, partial, include_global_state, ignore_unavailable, metadata
        // TODO validate date_format is of right format
    }

    override fun getName() = policyName

    override fun getLastUpdateTime() = jobLastUpdateTime

    override fun getEnabledTime() = jobEnabledTime

    override fun getSchedule() = jobSchedule

    override fun isEnabled() = jobEnabled

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder
            .startObject()
            .startObject(SM_TYPE)
            .field(NAME_FIELD, policyName)
            .optionalField(DESCRIPTION_FIELD, description)
            .field(CREATION_FIELD, creation)
            .field(DELETION_FIELD, deletion)
            .field(SNAPSHOT_CONFIG_FIELD, snapshotConfig)
            .field(SCHEDULE_FIELD, jobSchedule)
            .field(ENABLED_FIELD, jobEnabled)
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, jobLastUpdateTime)
            .optionalTimeField(ENABLED_TIME_FIELD, jobEnabledTime)
            .endObject()
            .endObject()
    }

    companion object {
        const val SM_TYPE = "sm"
        const val NAME_FIELD = "name"
        const val DESCRIPTION_FIELD = "description"
        const val CREATION_FIELD = "creation"
        const val DELETION_FIELD = "deletion"
        const val SNAPSHOT_CONFIG_FIELD = "snapshot_config"
        const val ENABLED_FIELD = "enabled"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val ENABLED_TIME_FIELD = "enabled_time"
        const val SCHEDULE_FIELD = "schedule"

        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            policyName: String? = null // meant to only be used by Index API
        ): SMPolicy {
            var name: String? = policyName
            var description: String? = null
            var creation: Creation? = null
            var deletion: Deletion? = null
            var snapshotConfig: Map<String, Any>? = null
            var lastUpdatedTime: Instant? = null
            var enabledTime: Instant? = null
            var schedule: Schedule? = null
            var enabled = true

            log.info("sm dev: first token:  ${xcp.currentToken()}, ${xcp.currentName()}")
            if (xcp.currentToken() == null) xcp.nextToken()
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                log.info("sm dev: current token loop: ${xcp.currentToken()}, ${xcp.currentName()}")
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME_FIELD -> name = xcp.text()
                    DESCRIPTION_FIELD -> description = xcp.nullValueHandler { text() }
                    CREATION_FIELD -> creation = Creation.parse(xcp)
                    DELETION_FIELD -> deletion = Deletion.parse(xcp)
                    SNAPSHOT_CONFIG_FIELD -> snapshotConfig = xcp.map()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = xcp.instant()
                    ENABLED_TIME_FIELD -> enabledTime = xcp.instant()
                    SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                }
            }

            if (enabled && enabledTime == null) {
                enabledTime = Instant.now()
            } else if (!enabled) {
                enabledTime = null
            }

            // TODO SM update policy API can update this value
            if (lastUpdatedTime == null) {
                lastUpdatedTime = Instant.now()
            }

            if (schedule == null) {
                schedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES)
            }

            return SMPolicy(
                policyName = requireNotNull(name) { "policy_name field must not be null" },
                description = description,
                creation = requireNotNull(creation) { "creation field must not be null" },
                deletion = requireNotNull(deletion) { "deletion field must not be null" },
                snapshotConfig = requireNotNull(snapshotConfig) { "snapshot_config field must not be null" },
                jobLastUpdateTime = requireNotNull(lastUpdatedTime) { "last_updated_at field must not be null" },
                jobEnabledTime = requireNotNull(enabledTime) { "job_enabled_time field must not be null" },
                jobSchedule = schedule,
                jobEnabled = enabled,
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm,
            )
        }
    }

    constructor(sin: StreamInput) : this(
        policyName = sin.readString(),
        description = sin.readOptionalString(),
        creation = Creation(sin),
        deletion = Deletion(sin),
        snapshotConfig = sin.readMap() as Map<String, Any>,
        jobLastUpdateTime = sin.readInstant(),
        jobEnabledTime = sin.readInstant(),
        jobSchedule = IntervalSchedule(sin),
        jobEnabled = sin.readBoolean(),
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(policyName)
        out.writeOptionalString(description)
        creation.writeTo(out)
        deletion.writeTo(out)
        out.writeMap(snapshotConfig)
        out.writeInstant(jobLastUpdateTime)
        out.writeInstant(jobEnabledTime)
        out.writeOptionalWriteable(jobSchedule)
        out.writeBoolean(jobEnabled)
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
    }

    data class Creation(
        val schedule: Schedule,
        val timeout: ActionTimeout? = null,
    ) : Writeable, ToXContent {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(SCHEDULE_FIELD, schedule)
                .optionalField(TIMEOUT_FIELD, timeout)
                .endObject()
        }

        companion object {
            const val SCHEDULE_FIELD = "schedule"
            const val TIMEOUT_FIELD = "timeout"

            fun parse(xcp: XContentParser): Creation {
                var schedule: Schedule? = null
                var timeout: ActionTimeout? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                        TIMEOUT_FIELD -> timeout = ActionTimeout.parse(xcp)
                    }
                }

                return Creation(
                    schedule = requireNotNull(schedule) { "schedule field must not be null" },
                    timeout = timeout
                )
            }
        }

        constructor(sin: StreamInput) : this(
            schedule = CronSchedule(sin),
            timeout = sin.readOptionalWriteable(::ActionTimeout),
        )

        override fun writeTo(out: StreamOutput) {
            schedule.writeTo(out)
            out.writeOptionalWriteable(timeout)
        }
    }

    data class Deletion(
        val schedule: Schedule,
        val timeout: ActionTimeout? = null,
        val condition: DeleteCondition,
    ) : Writeable, ToXContent {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(SCHEDULE_FIELD, schedule)
                .optionalField(TIMEOUT_FIELD, timeout)
                .field(CONDITION_FIELD, condition)
                .endObject()
        }

        companion object {
            const val SCHEDULE_FIELD = "schedule"
            const val TIMEOUT_FIELD = "timeout"
            const val CONDITION_FIELD = "condition"

            fun parse(xcp: XContentParser): Deletion {
                var schedule: Schedule? = null
                var timeout: ActionTimeout? = null
                var condition: DeleteCondition? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                        TIMEOUT_FIELD -> timeout = ActionTimeout.parse(xcp)
                        CONDITION_FIELD -> condition = DeleteCondition.parse(xcp)
                    }
                }

                // If user doesn't provide delete schedule, defaults to every day 1AM
                if (schedule == null) {
                    schedule = CronSchedule("0 1 * * *", ZoneId.systemDefault())
                }

                return Deletion(
                    schedule = schedule,
                    timeout = timeout,
                    condition = requireNotNull(condition) { "condition field must not be null" },
                )
            }
        }

        constructor(sin: StreamInput) : this(
            schedule = CronSchedule(sin),
            timeout = sin.readOptionalWriteable(::ActionTimeout),
            condition = DeleteCondition(sin),
        )

        override fun writeTo(out: StreamOutput) {
            schedule.writeTo(out)
            out.writeOptionalWriteable(timeout)
            condition.writeTo(out)
        }
    }

    data class DeleteCondition(
        val maxCount: Int,
        val maxAge: TimeValue? = null,
        val minCount: Int? = null,
    ) : Writeable, ToXContent {

        init {
            require(
                (maxAge != null && minCount != null) || (maxAge == null && minCount == null)
            ) { "max_age and min_count should exist at the same time." }
            require(minCount == null || maxCount > minCount) {
                "max_count should be bigger than min_count."
            }
        }

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(MAX_COUNT_FIELD, maxCount)
                .optionalField(MAX_AGE_FIELD, maxAge)
                .optionalField(MIN_COUNT_FIELD, minCount)
                .endObject()
        }

        companion object {
            const val MAX_COUNT_FIELD = "max_count"
            const val MAX_AGE_FIELD = "max_age"
            const val MIN_COUNT_FIELD = "min_count"

            fun parse(xcp: XContentParser): DeleteCondition {
                var maxCount: Int? = null
                var maxAge: TimeValue? = null
                var minCount: Int? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        MAX_COUNT_FIELD -> maxCount = xcp.intValue()
                        MAX_AGE_FIELD -> maxAge = TimeValue.parseTimeValue(xcp.text(), MAX_AGE_FIELD)
                        MIN_COUNT_FIELD -> minCount = xcp.intValue()
                    }
                }

                return DeleteCondition(
                    maxCount = requireNotNull(maxCount) { "max_count field must not be null" },
                    maxAge = maxAge,
                    minCount = minCount,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            maxCount = sin.readInt(),
            maxAge = sin.readOptionalTimeValue(),
            minCount = sin.readOptionalInt()
        )

        override fun writeTo(out: StreamOutput) {
            out.writeInt(maxCount)
            out.writeOptionalTimeValue(maxAge)
            out.writeOptionalInt(minCount)
        }
    }
}
