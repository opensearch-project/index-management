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
import org.opensearch.indexmanagement.util.IndexUtils.Companion.NO_ID
import org.opensearch.indexmanagement.util.ScheduleType
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.jobscheduler.spi.schedule.Schedule
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser
import java.time.Instant
import java.time.temporal.ChronoUnit

private val log = LogManager.getLogger(SM::class.java)

data class SM(
    val policyName: String,
    val description: String?,
    val jobEnabled: Boolean,
    val jobLastUpdateTime: Instant,
    val createSchedule: Schedule, // TODO Required
    val snapshotConfig: Map<String, Any>,
    val deleteSchedule: Schedule, // TODO Optional if not provided, default to every day midnight
    val deleteCondition: DeleteCondition,
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val jobEnabledTime: Instant,
    val jobSchedule: Schedule,
) : ScheduledJobParameter, Writeable {

    // TODO validate snapshotConfig
    init {
        require(snapshotConfig["repository"] != null) { "Must provide a repository." }
        // indices, partial, include_global_state, ignore_unavailable, metadata
    }

    override fun getName() = policyName

    override fun getLastUpdateTime() = jobLastUpdateTime

    override fun getEnabledTime() = jobEnabledTime

    override fun getSchedule() = jobSchedule

    override fun isEnabled() = jobEnabled

    // use to save in document
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder
            .startObject()
            .startObject(SM_TYPE)
            .field(NAME_FIELD, policyName)
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, jobLastUpdateTime)
            .optionalTimeField(ENABLED_TIME_FIELD, jobEnabledTime)
            .field(SCHEDULE_FIELD, jobSchedule)
            .field(ENABLED_FIELD, jobEnabled)
            .field(CREATE_SCHEDULE_FIELD, createSchedule)
            .field(SNAPSHOT_CONFIG_FIELD, snapshotConfig)
            .field(DELETE_SCHEDULE_FIELD, deleteSchedule)
            .field(DELETE_CONDITION_FIELD, deleteCondition)
            .endObject()
            .endObject()
    }

    companion object {
        const val SM_TYPE = "sm"
        const val NAME_FIELD = "name"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val ENABLED_TIME_FIELD = "enabled_time"
        const val SCHEDULE_FIELD = "schedule"
        const val ENABLED_FIELD = "enabled"
        const val CREATE_SCHEDULE_FIELD = "create_schedule"
        const val SNAPSHOT_CONFIG_FIELD = "snapshot_config"
        const val DELETE_SCHEDULE_FIELD = "delete_schedule"
        const val DELETE_CONDITION_FIELD = "delete_condition"

        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            policyName: String? = null
        ): SM {
            var name: String? = policyName
            var lastUpdatedTime: Instant? = null
            var enabledTime: Instant? = null
            var schedule: Schedule? = null
            var enabled = true
            var createSchedule: Schedule? = null // TODO default to some schedule?
            var snapshotConfig: Map<String, Any>? = null
            var deleteSchedule: Schedule? = null // default
            var deleteCondition: DeleteCondition? = null // default

            log.info("first token:  ${xcp.currentToken()}, ${xcp.currentName()}")
            if (xcp.currentToken() == null) xcp.nextToken()
            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                log.info("current token loop: ${xcp.currentToken()}, ${xcp.currentName()}")
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME_FIELD -> name = xcp.text()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = xcp.instant()
                    ENABLED_TIME_FIELD -> enabledTime = xcp.instant()
                    SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                    CREATE_SCHEDULE_FIELD -> createSchedule = ScheduleParser.parse(xcp)
                    SNAPSHOT_CONFIG_FIELD -> snapshotConfig = xcp.map()
                    DELETE_SCHEDULE_FIELD -> deleteSchedule = ScheduleParser.parse(xcp)
                    DELETE_CONDITION_FIELD -> deleteCondition = DeleteCondition.parse(xcp)
                }
            }

            if (enabled && enabledTime == null) {
                enabledTime = Instant.now()
            } else if (!enabled) {
                enabledTime = null
            }

            // TODO what can change this field?
            //  in the code change job? theoretically, we don't want to do this
            //  user use update policy API
            if (lastUpdatedTime == null) {
                lastUpdatedTime = Instant.now()
            }

            if (schedule == null) {
                schedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES)
            }

            return SM(
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm,
                policyName = requireNotNull(name) { "policy_name field must not be null" },
                jobLastUpdateTime = requireNotNull(lastUpdatedTime) { "last_updated_at field must not be null" },
                jobEnabledTime = requireNotNull(enabledTime) { "job_enabled_time field must not be null" },
                jobSchedule = schedule,
                jobEnabled = enabled,
                description = null,
                createSchedule = requireNotNull(createSchedule) { "create_schedule field must not be null" },
                snapshotConfig = requireNotNull(snapshotConfig) { "snapshot_config field must not be null" },
                deleteSchedule = requireNotNull(deleteSchedule) { "delete_schedule field must not be null" },
                deleteCondition = requireNotNull(deleteCondition) { "delete_condition field must not be null" }
            )
        }
    }

    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        policyName = sin.readString(),
        jobLastUpdateTime = sin.readInstant(),
        jobEnabledTime = sin.readInstant(),
        jobSchedule = requireNotNull(sin.readOptionalWriteable(::IntervalSchedule)),
        jobEnabled = sin.readBoolean(),
        description = null,
        createSchedule = sin.let {
            when (requireNotNull(sin.readEnum(ScheduleType::class.java)) { "ScheduleType cannot be null" }) {
                ScheduleType.CRON -> CronSchedule(sin)
                ScheduleType.INTERVAL -> IntervalSchedule(sin)
            }
        },
        snapshotConfig = sin.readMap() as Map<String, Any>,
        deleteSchedule = sin.let {
            when (requireNotNull(sin.readEnum(ScheduleType::class.java)) { "ScheduleType cannot be null" }) {
                ScheduleType.CRON -> CronSchedule(sin)
                ScheduleType.INTERVAL -> IntervalSchedule(sin)
            }
        },
        deleteCondition = DeleteCondition(sin)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeString(policyName)
        out.writeInstant(jobLastUpdateTime)
        out.writeInstant(jobEnabledTime)
        out.writeOptionalWriteable(jobSchedule)
        out.writeBoolean(jobEnabled)
        if (createSchedule is CronSchedule) {
            out.writeEnum(ScheduleType.CRON)
        } else {
            out.writeEnum(ScheduleType.INTERVAL)
        }
        createSchedule.writeTo(out)
        out.writeMap(snapshotConfig)
        if (deleteSchedule is CronSchedule) {
            out.writeEnum(ScheduleType.CRON)
        } else {
            out.writeEnum(ScheduleType.INTERVAL)
        }
        deleteSchedule.writeTo(out)
        deleteCondition.writeTo(out)
    }
}

data class DeleteCondition(
    val age: String, // TODO this is optional
    val count: List<Int>
) : Writeable, ToXContentFragment {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(AGE_FIELD, age)
            .field(COUNT_FIELD, count)
            .endObject()
    }

    companion object {
        const val AGE_FIELD = "age"
        const val COUNT_FIELD = "count"

        fun parse(xcp: XContentParser): DeleteCondition {
            var age: String? = null
            var count: List<Int>? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    AGE_FIELD -> age = xcp.text()
                    COUNT_FIELD -> count = xcp.parseArray { intValue() }
                }
            }

            return DeleteCondition(
                age = requireNotNull(age) { "age field must not be null" },
                count = requireNotNull(count) { "count field must not be null" }
            )
        }
    }

    constructor(sin: StreamInput) : this(
        sin.readString(),
        sin.readIntArray().toList()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(age)
        out.writeIntArray(count.toIntArray())
    }
}
