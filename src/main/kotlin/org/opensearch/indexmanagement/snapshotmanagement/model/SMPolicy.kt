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
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.opensearchapi.instant
import org.opensearch.indexmanagement.opensearchapi.nullValueHandler
import org.opensearch.indexmanagement.opensearchapi.optionalField
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import org.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToMetadataId
import org.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToMetadataId
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.DeleteCondition.Companion.DEFAULT_DELETE_CONDITION
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
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
    val id: String,
    val description: String? = null,
    val creation: Creation,
    val deletion: Deletion?,
    val snapshotConfig: Map<String, Any>,
    val jobEnabled: Boolean,
    val jobLastUpdateTime: Instant,
    val jobEnabledTime: Instant?,
    val jobSchedule: Schedule,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
) : ScheduledJobParameter, Writeable {

    init {
        require(snapshotConfig["repository"] != null) { "Must provide the repository in snapshot config." }
    }

    // This name is used by the job scheduler and needs to match the id to avoid namespace conflicts with ISM policies sharing the same name
    override fun getName() = id

    // This is the name which the user provided when creating the policy, and should be used when outputting to the user in REST responses
    val policyName get() = smDocIdToPolicyName(id)

    val metadataID get() = smPolicyNameToMetadataId(smDocIdToPolicyName(id))

    override fun getLastUpdateTime() = jobLastUpdateTime

    override fun getEnabledTime() = jobEnabledTime

    override fun getSchedule() = jobSchedule

    override fun isEnabled() = jobEnabled

    override fun getLockDurationSeconds() = 1800L // 30 minutes

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(SM_TYPE)
        builder.field(NAME_FIELD, smDocIdToPolicyName(id)) // for searching policy by name
            .optionalField(DESCRIPTION_FIELD, description)
            .field(CREATION_FIELD, creation)
            .optionalField(DELETION_FIELD, deletion)
            .field(SNAPSHOT_CONFIG_FIELD, snapshotConfig)
            .field(SCHEDULE_FIELD, jobSchedule)
            .field(ENABLED_FIELD, jobEnabled)
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, jobLastUpdateTime)
            .optionalTimeField(ENABLED_TIME_FIELD, jobEnabledTime)
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        return builder.endObject()
    }

    companion object {
        const val SM_TYPE = "sm_policy"
        const val SM_DOC_ID_SUFFIX = "-sm-policy"
        const val SM_METADATA_ID_SUFFIX = "-sm-metadata"
        const val NAME_FIELD = "name"
        const val DESCRIPTION_FIELD = "description"
        const val CREATION_FIELD = "creation"
        const val DELETION_FIELD = "deletion"
        const val SNAPSHOT_CONFIG_FIELD = "snapshot_config"
        const val DATE_FORMAT_FIELD = "date_format"
        const val ENABLED_FIELD = "enabled"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val ENABLED_TIME_FIELD = "enabled_time"
        const val SCHEDULE_FIELD = "schedule"

        // Used by sub models Creation and Deletion
        const val TIME_LIMIT_FIELD = "time_limit"

        fun parse(
            xcp: XContentParser,
            id: String,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
        ): SMPolicy {
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
                    NAME_FIELD -> requireNotNull(xcp.text()) { "The name field of SMPolicy must not be null." }
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

            require(creation != null) { "Must provide the schedule for snapshot creation." }
            // If user doesn't provide delete schedule, use the creation schedule
            if (deletion != null && !deletion.scheduleProvided) {
                deletion = deletion.copy(
                    schedule = creation.schedule
                )
            }

            return SMPolicy(
                description = description,
                creation = creation,
                deletion = deletion,
                snapshotConfig = requireNotNull(snapshotConfig) { "snapshot_config field must not be null" },
                jobLastUpdateTime = requireNotNull(lastUpdatedTime) { "last_updated_at field must not be null" },
                jobEnabledTime = enabledTime,
                jobSchedule = schedule,
                jobEnabled = enabled,
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm,
            )
        }
    }

    constructor(sin: StreamInput) : this(
        description = sin.readOptionalString(),
        creation = Creation(sin),
        deletion = sin.readOptionalWriteable { Deletion(it) },
        snapshotConfig = sin.readMap() as Map<String, Any>,
        jobLastUpdateTime = sin.readInstant(),
        jobEnabledTime = sin.readOptionalInstant(),
        jobSchedule = IntervalSchedule(sin),
        jobEnabled = sin.readBoolean(),
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(description)
        creation.writeTo(out)
        out.writeOptionalWriteable(deletion)
        out.writeMap(snapshotConfig)
        out.writeInstant(jobLastUpdateTime)
        out.writeOptionalInstant(jobEnabledTime)
        jobSchedule.writeTo(out)
        out.writeBoolean(jobEnabled)
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
    }

    data class Creation(
        val schedule: Schedule,
        val timeLimit: TimeValue? = null,
    ) : Writeable, ToXContent {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(SCHEDULE_FIELD, schedule)
                .optionalField(TIME_LIMIT_FIELD, timeLimit)
                .endObject()
        }

        companion object {
            const val SCHEDULE_FIELD = "schedule"

            fun parse(xcp: XContentParser): Creation {
                var schedule: Schedule? = null
                var timeLimit: TimeValue? = null

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                        TIME_LIMIT_FIELD -> timeLimit = TimeValue.parseTimeValue(xcp.text(), TIME_LIMIT_FIELD)
                    }
                }

                return Creation(
                    schedule = requireNotNull(schedule) { "schedule field must not be null" },
                    timeLimit = timeLimit
                )
            }
        }

        constructor(sin: StreamInput) : this(
            schedule = CronSchedule(sin),
            timeLimit = sin.readOptionalTimeValue(),
        )

        override fun writeTo(out: StreamOutput) {
            schedule.writeTo(out)
            out.writeOptionalTimeValue(timeLimit)
        }
    }

    data class Deletion(
        val schedule: Schedule,
        val scheduleProvided: Boolean = true,
        val condition: DeleteCondition,
        val timeLimit: TimeValue? = null,
    ) : Writeable, ToXContent {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(SCHEDULE_FIELD, schedule)
                .field(CONDITION_FIELD, condition)
                .optionalField(TIME_LIMIT_FIELD, timeLimit)
                .endObject()
        }

        companion object {
            const val SCHEDULE_FIELD = "schedule"
            const val CONDITION_FIELD = "condition"

            fun parse(xcp: XContentParser): Deletion {
                var schedule: Schedule? = null
                var timeLimit: TimeValue? = null
                var condition: DeleteCondition? = null
                var scheduleProvided = true

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                        TIME_LIMIT_FIELD -> timeLimit = TimeValue.parseTimeValue(xcp.text(), TIME_LIMIT_FIELD)
                        CONDITION_FIELD -> condition = DeleteCondition.parse(xcp)
                    }
                }

                if (schedule == null) {
                    // This schedule is just a placeholder
                    schedule = CronSchedule("0 1 * * *", ZoneId.systemDefault())
                    scheduleProvided = false
                }

                if (condition == null) {
                    condition = DEFAULT_DELETE_CONDITION
                }

                return Deletion(
                    schedule = schedule,
                    scheduleProvided = scheduleProvided,
                    timeLimit = timeLimit,
                    condition = condition,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            schedule = CronSchedule(sin),
            timeLimit = sin.readOptionalTimeValue(),
            condition = DeleteCondition(sin),
        )

        override fun writeTo(out: StreamOutput) {
            schedule.writeTo(out)
            out.writeOptionalTimeValue(timeLimit)
            condition.writeTo(out)
        }
    }

    data class DeleteCondition(
        val maxAge: TimeValue? = null,
        val minCount: Int,
        val maxCount: Int? = null,
    ) : Writeable, ToXContent {

        init {
            require(minCount > 0) { "$MIN_COUNT_FIELD should be bigger than 0." }
            require(maxCount == null || maxCount - minCount > 0) { "$MAX_COUNT_FIELD should be bigger than $MIN_COUNT_FIELD." }
        }

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .optionalField(MAX_AGE_FIELD, maxAge)
                .field(MIN_COUNT_FIELD, minCount)
                .optionalField(MAX_COUNT_FIELD, maxCount)
                .endObject()
        }

        companion object {
            const val MAX_COUNT_FIELD = "max_count"
            const val MAX_AGE_FIELD = "max_age"
            const val MIN_COUNT_FIELD = "min_count"
            private const val DEFAULT_MIN_COUNT = 1

            val DEFAULT_DELETE_CONDITION = DeleteCondition(minCount = DEFAULT_MIN_COUNT)

            fun parse(xcp: XContentParser): DeleteCondition {
                var maxAge: TimeValue? = null
                var minCount: Int = DEFAULT_MIN_COUNT
                var maxCount: Int? = null

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
                    maxCount = maxCount,
                    maxAge = maxAge,
                    minCount = minCount,
                )
            }
        }

        constructor(sin: StreamInput) : this(
            maxCount = sin.readOptionalInt(),
            maxAge = sin.readOptionalTimeValue(),
            minCount = sin.readInt()
        )

        override fun writeTo(out: StreamOutput) {
            out.writeOptionalInt(maxCount)
            out.writeOptionalTimeValue(maxAge)
            out.writeInt(minCount)
        }
    }
}
