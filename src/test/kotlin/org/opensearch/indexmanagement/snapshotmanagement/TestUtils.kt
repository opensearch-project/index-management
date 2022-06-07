/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.opensearch.Version
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import org.opensearch.action.index.IndexResponse
import org.opensearch.cluster.SnapshotsInProgress
import org.opensearch.common.UUIDs
import org.opensearch.common.collect.ImmutableOpenMap
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.opensearchapi.toMap
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.randomIntervalSchedule
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.snapshots.Snapshot
import org.opensearch.snapshots.SnapshotId
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength
import org.opensearch.test.OpenSearchTestCase.randomIntBetween
import org.opensearch.test.OpenSearchTestCase.randomNonNegativeLong
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.time.Instant
import java.time.ZoneOffset.UTC

fun randomSMMetadata(
    currentState: SMState = SMState.START,
    nextCreationTime: Instant = randomInstant(),
    nextDeletionTime: Instant = randomInstant(),
    policySeqNo: Long = randomNonNegativeLong(),
    policyPrimaryTerm: Long = randomNonNegativeLong(),
    startedCreation: String? = null,
    creationLatestExecution: SMMetadata.LatestExecution? = null,
    startedDeletion: List<String>? = null,
    deletionLatestExecution: SMMetadata.LatestExecution? = null,
    creationRetryCount: Int? = null,
    deletionRetryCount: Int? = null,
): SMMetadata {
    return SMMetadata(
        policySeqNo = policySeqNo,
        policyPrimaryTerm = policyPrimaryTerm,
        currentState = currentState,
        creation = SMMetadata.WorkflowMetadata(
            trigger = SMMetadata.Trigger(
                time = nextCreationTime
            ),
            started = if (startedCreation != null) listOf(startedCreation) else null,
            latestExecution = creationLatestExecution,
            retry = creationRetryCount?.let { SMMetadata.Retry(it) },
        ),
        deletion = SMMetadata.WorkflowMetadata(
            trigger = SMMetadata.Trigger(
                time = nextDeletionTime
            ),
            started = startedDeletion,
            latestExecution = deletionLatestExecution,
            retry = deletionRetryCount?.let { SMMetadata.Retry(it) },
        ),
    )
}

fun randomLatestExecution(
    status: SMMetadata.LatestExecution.Status = SMMetadata.LatestExecution.Status.IN_PROGRESS,
    startTime: Instant = randomInstant(),
) = SMMetadata.LatestExecution(
    status = status,
    startTime = startTime,
)

fun randomSMPolicy(
    policyName: String = randomAlphaOfLength(10),
    jobEnabled: Boolean = OpenSearchRestTestCase.randomBoolean(),
    jobLastUpdateTime: Instant = randomInstant(),
    creationSchedule: CronSchedule = CronSchedule("* * * * *", UTC),
    creationTimeLimit: TimeValue? = null,
    deletionSchedule: CronSchedule = CronSchedule("* * * * *", UTC),
    deletionTimeLimit: TimeValue? = null,
    deletionMaxCount: Int = randomIntBetween(5, 10),
    deletionMaxAge: TimeValue? = null,
    deletionMinCount: Int? = null,
    snapshotConfig: Map<String, Any> = mapOf(
        "repository" to "repo",
        "date_format" to "yyyy-MM-dd-HH:mm"
    ),
    jobEnabledTime: Instant? = randomInstant(),
    jobSchedule: IntervalSchedule = randomIntervalSchedule(),
    seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
): SMPolicy {
    return SMPolicy(
        id = smPolicyNameToDocId(policyName),
        jobEnabled = jobEnabled,
        jobLastUpdateTime = jobLastUpdateTime,
        creation = SMPolicy.Creation(
            schedule = creationSchedule,
            timeLimit = creationTimeLimit,
        ),
        deletion = SMPolicy.Deletion(
            schedule = deletionSchedule,
            timeLimit = deletionTimeLimit,
            condition = SMPolicy.DeleteCondition(
                maxCount = deletionMaxCount,
                maxAge = deletionMaxAge,
                minCount = deletionMinCount
            )
        ),
        snapshotConfig = snapshotConfig,
        jobEnabledTime = if (jobEnabled) jobEnabledTime else null,
        jobSchedule = jobSchedule,
        seqNo = seqNo,
        primaryTerm = primaryTerm,
    )
}

fun randomSnapshotName(): String = randomAlphaOfLength(10)

fun ToXContent.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params).string()

fun ToXContent.toMap(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): Map<String, Any> = this.toXContent(XContentFactory.jsonBuilder(), params).toMap()

fun mockIndexResponse(status: RestStatus = RestStatus.OK): IndexResponse {
    val indexResponse: IndexResponse = mock()
    whenever(indexResponse.status()).doReturn(status)
    whenever(indexResponse.seqNo).doReturn(0L)
    whenever(indexResponse.primaryTerm).doReturn(1L)

    return indexResponse
}

fun mockCreateSnapshotResponse(status: RestStatus = RestStatus.ACCEPTED): CreateSnapshotResponse {
    val createSnapshotRes: CreateSnapshotResponse = mock()
    whenever(createSnapshotRes.status()).doReturn(status)
    return createSnapshotRes
}

fun mockGetSnapshotResponse(snapshotInfo: SnapshotInfo): GetSnapshotsResponse {
    val getSnapshotsRes: GetSnapshotsResponse = mock()
    whenever(getSnapshotsRes.snapshots).doReturn(listOf(snapshotInfo))
    return getSnapshotsRes
}

fun mockGetSnapshotResponse(snapshotInfos: List<SnapshotInfo>): GetSnapshotsResponse {
    val getSnapshotsRes: GetSnapshotsResponse = mock()
    whenever(getSnapshotsRes.snapshots).doReturn(snapshotInfos)
    return getSnapshotsRes
}

fun mockSnapshotInfo(
    name: String = randomAlphaOfLength(10),
    startTime: Long = randomNonNegativeLong(),
    endTime: Long = randomNonNegativeLong(),
    reason: String? = null, // reason with valid string leads to FAILED snapshot state
    policyName: String = "daily-snapshot",
): SnapshotInfo {
    val result = SnapshotInfo(
        SnapshotId(name, UUIDs.randomBase64UUID()),
        listOf("index1"),
        listOf("ds-1"),
        startTime,
        reason,
        endTime,
        5,
        emptyList(),
        false,
        mapOf("sm_policy" to policyName),
    )
    return result
}

/**
 * SnapshotInfo is final class so we cannot directly mock
 *
 * Need to mock the InProgress state and snapshot metadata
 */
fun mockInProgressSnapshotInfo(
    name: String = randomAlphaOfLength(10),
): SnapshotInfo {
    val entry = SnapshotsInProgress.Entry(
        Snapshot("repo", SnapshotId(name, UUIDs.randomBase64UUID())),
        false,
        false,
        SnapshotsInProgress.State.SUCCESS,
        emptyList(),
        emptyList(),
        randomNonNegativeLong(),
        randomNonNegativeLong(),
        ImmutableOpenMap.of(),
        "",
        mapOf("sm_policy" to "daily-snapshot"),
        Version.CURRENT,
    )
    return SnapshotInfo(entry)
}

fun mockGetSnapshotResponse(num: Int): GetSnapshotsResponse {
    val getSnapshotsRes: GetSnapshotsResponse = mock()
    whenever(getSnapshotsRes.snapshots).doReturn(mockSnapshotInfoList(num))
    return getSnapshotsRes
}

fun mockSnapshotInfoList(num: Int, namePrefix: String = randomAlphaOfLength(10)): List<SnapshotInfo> {
    val result = mutableListOf<SnapshotInfo>()
    for (i in 1..num) {
        result.add(
            mockSnapshotInfo(
                name = namePrefix + i
            )
        )
    }
    return result.toList()
}
