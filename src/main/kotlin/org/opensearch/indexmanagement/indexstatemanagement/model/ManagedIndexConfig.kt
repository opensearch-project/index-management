/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import org.opensearch.indexmanagement.opensearchapi.instant
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.schedule.Schedule
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser
import java.io.IOException
import java.time.Instant

data class ManagedIndexConfig(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val jobName: String,
    val index: String,
    val indexUuid: String,
    val enabled: Boolean,
    val jobSchedule: Schedule,
    val jobLastUpdatedTime: Instant,
    val jobEnabledTime: Instant?,
    val policyID: String,
    val policySeqNo: Long?,
    val policyPrimaryTerm: Long?,
    val policy: Policy?,
    val changePolicy: ChangePolicy?,
    val jobJitter: Double?
) : ScheduledJobParameter {

    init {
        if (enabled) {
            requireNotNull(jobEnabledTime) { "jobEnabledTime must be present if the job is enabled" }
        } else {
            require(jobEnabledTime == null) { "jobEnabledTime must not be present if the job is disabled" }
        }
    }

    override fun isEnabled() = enabled

    override fun getName() = jobName

    override fun getEnabledTime() = jobEnabledTime

    override fun getSchedule() = jobSchedule

    override fun getLastUpdateTime() = jobLastUpdatedTime

    override fun getLockDurationSeconds(): Long = 3600L // 1 hour

    override fun getJitter(): Double? {
        return jobJitter
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
            .startObject(MANAGED_INDEX_TYPE)
            .field(NAME_FIELD, jobName)
            .field(ENABLED_FIELD, enabled)
            .field(INDEX_FIELD, index)
            .field(INDEX_UUID_FIELD, indexUuid)
            .field(SCHEDULE_FIELD, jobSchedule)
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, jobLastUpdatedTime)
            .optionalTimeField(ENABLED_TIME_FIELD, jobEnabledTime)
            .field(POLICY_ID_FIELD, policyID)
            .field(POLICY_SEQ_NO_FIELD, policySeqNo)
            .field(POLICY_PRIMARY_TERM_FIELD, policyPrimaryTerm)
            .field(POLICY_FIELD, policy, XCONTENT_WITHOUT_TYPE)
            .field(CHANGE_POLICY_FIELD, changePolicy)
            .field(JITTER, jobJitter)
        builder.endObject()
        return builder.endObject()
    }

    companion object {
        const val MANAGED_INDEX_TYPE = "managed_index"
        const val NAME_FIELD = "name"
        const val ENABLED_FIELD = "enabled"
        const val SCHEDULE_FIELD = "schedule"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val ENABLED_TIME_FIELD = "enabled_time"
        const val INDEX_FIELD = "index"
        const val INDEX_UUID_FIELD = "index_uuid"
        const val POLICY_ID_FIELD = "policy_id"
        const val POLICY_FIELD = "policy"
        const val POLICY_SEQ_NO_FIELD = "policy_seq_no"
        const val POLICY_PRIMARY_TERM_FIELD = "policy_primary_term"
        const val CHANGE_POLICY_FIELD = "change_policy"
        const val JITTER = "jitter"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): ManagedIndexConfig {
            var name: String? = null
            var index: String? = null
            var indexUuid: String? = null
            var schedule: Schedule? = null
            var policyID: String? = null
            var policy: Policy? = null
            var changePolicy: ChangePolicy? = null
            var lastUpdatedTime: Instant? = null
            var enabledTime: Instant? = null
            var enabled = true
            var policyPrimaryTerm: Long? = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
            var policySeqNo: Long? = SequenceNumbers.UNASSIGNED_SEQ_NO
            var jitter: Double? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME_FIELD -> name = xcp.text()
                    INDEX_FIELD -> index = xcp.text()
                    INDEX_UUID_FIELD -> indexUuid = xcp.text()
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                    SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                    ENABLED_TIME_FIELD -> enabledTime = xcp.instant()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = xcp.instant()
                    POLICY_ID_FIELD -> policyID = xcp.text()
                    POLICY_SEQ_NO_FIELD -> {
                        policySeqNo = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.longValue()
                    }
                    POLICY_PRIMARY_TERM_FIELD -> {
                        policyPrimaryTerm = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.longValue()
                    }
                    POLICY_FIELD -> {
                        policy = if (xcp.currentToken() == Token.VALUE_NULL) null else Policy.parse(xcp)
                    }
                    CHANGE_POLICY_FIELD -> {
                        changePolicy = if (xcp.currentToken() == Token.VALUE_NULL) null else ChangePolicy.parse(xcp)
                    }
                    JITTER -> {
                        jitter = if (xcp.currentToken() == Token.VALUE_NULL) null else xcp.doubleValue()
                    }
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ManagedIndexConfig.")
                }
            }

            if (enabled && enabledTime == null) {
                enabledTime = Instant.now()
            } else if (!enabled) {
                enabledTime = null
            }
            return ManagedIndexConfig(
                id,
                seqNo,
                primaryTerm,
                index = requireNotNull(index) { "ManagedIndexConfig index is null" },
                indexUuid = requireNotNull(indexUuid) { "ManagedIndexConfig index uuid is null" },
                jobName = requireNotNull(name) { "ManagedIndexConfig name is null" },
                enabled = enabled,
                jobSchedule = requireNotNull(schedule) { "ManagedIndexConfig schedule is null" },
                jobLastUpdatedTime = requireNotNull(lastUpdatedTime) { "ManagedIndexConfig last updated time is null" },
                jobEnabledTime = enabledTime,
                policyID = requireNotNull(policyID) { "ManagedIndexConfig policy id is null" },
                policySeqNo = policySeqNo,
                policyPrimaryTerm = policyPrimaryTerm,
                policy = policy?.copy(
                    id = policyID ?: NO_ID,
                    seqNo = policySeqNo ?: SequenceNumbers.UNASSIGNED_SEQ_NO,
                    primaryTerm = policyPrimaryTerm ?: SequenceNumbers.UNASSIGNED_PRIMARY_TERM
                ),
                changePolicy = changePolicy,
                jobJitter = jitter
            )
        }
    }
}
