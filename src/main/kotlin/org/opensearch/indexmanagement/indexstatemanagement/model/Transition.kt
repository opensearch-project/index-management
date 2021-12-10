/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser
import java.io.IOException

data class Transition(
    val stateName: String,
    val conditions: Conditions?
) : ToXContentObject, Writeable {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(STATE_NAME_FIELD, stateName)
        if (conditions != null) builder.field(CONDITIONS_FIELD, conditions)
        return builder.endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        stateName = sin.readString(),
        conditions = sin.readOptionalWriteable(::Conditions)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(stateName)
        out.writeOptionalWriteable(conditions)
    }

    companion object {
        const val STATE_NAME_FIELD = "state_name"
        const val CONDITIONS_FIELD = "conditions"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Transition {
            var name: String? = null
            var conditions: Conditions? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    STATE_NAME_FIELD -> name = xcp.text()
                    CONDITIONS_FIELD -> conditions = Conditions.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Transition.")
                }
            }

            return Transition(
                stateName = requireNotNull(name) { "Transition state name is null" },
                conditions = conditions
            )
        }
    }
}

data class Conditions(
    val indexAge: TimeValue? = null,
    val docCount: Long? = null,
    val size: ByteSizeValue? = null,
    val cron: CronSchedule? = null,
    val rolloverAge: TimeValue? = null
) : ToXContentObject, Writeable {

    init {
        val conditionsList = listOf(indexAge, docCount, size, cron, rolloverAge)
        require(conditionsList.filterNotNull().size == 1) { "Cannot provide more than one Transition condition" }

        // Validate doc count condition
        if (docCount != null) require(docCount > 0) { "Transition doc count condition must be greater than 0" }

        // Validate size condition
        if (size != null) require(size.bytes > 0) { "Transition size condition must be greater than 0" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (indexAge != null) builder.field(MIN_INDEX_AGE_FIELD, indexAge.stringRep)
        if (docCount != null) builder.field(MIN_DOC_COUNT_FIELD, docCount)
        if (size != null) builder.field(MIN_SIZE_FIELD, size.stringRep)
        if (cron != null) builder.field(CRON_FIELD, cron)
        if (rolloverAge != null) builder.field(MIN_ROLLOVER_AGE_FIELD, rolloverAge.stringRep)
        return builder.endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indexAge = sin.readOptionalTimeValue(),
        docCount = sin.readOptionalLong(),
        size = sin.readOptionalWriteable(::ByteSizeValue),
        cron = sin.readOptionalWriteable(::CronSchedule),
        rolloverAge = sin.readOptionalTimeValue()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalTimeValue(indexAge)
        out.writeOptionalLong(docCount)
        out.writeOptionalWriteable(size)
        out.writeOptionalWriteable(cron)
        out.writeOptionalTimeValue(rolloverAge)
    }

    companion object {
        const val MIN_INDEX_AGE_FIELD = "min_index_age"
        const val MIN_DOC_COUNT_FIELD = "min_doc_count"
        const val MIN_SIZE_FIELD = "min_size"
        const val CRON_FIELD = "cron"
        const val MIN_ROLLOVER_AGE_FIELD = "min_rollover_age"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Conditions {
            var indexAge: TimeValue? = null
            var docCount: Long? = null
            var size: ByteSizeValue? = null
            var cron: CronSchedule? = null
            var rolloverAge: TimeValue? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    MIN_INDEX_AGE_FIELD -> indexAge = TimeValue.parseTimeValue(xcp.text(), MIN_INDEX_AGE_FIELD)
                    MIN_DOC_COUNT_FIELD -> docCount = xcp.longValue()
                    MIN_SIZE_FIELD -> size = ByteSizeValue.parseBytesSizeValue(xcp.text(), MIN_SIZE_FIELD)
                    CRON_FIELD -> cron = ScheduleParser.parse(xcp) as? CronSchedule
                    MIN_ROLLOVER_AGE_FIELD -> rolloverAge = TimeValue.parseTimeValue(xcp.text(), MIN_ROLLOVER_AGE_FIELD)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Conditions.")
                }
            }

            return Conditions(indexAge, docCount, size, cron, rolloverAge)
        }
    }
}
