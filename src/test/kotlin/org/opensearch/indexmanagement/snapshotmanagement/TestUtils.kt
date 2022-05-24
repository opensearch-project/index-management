/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import org.opensearch.action.index.IndexResponse
import org.opensearch.common.UUIDs
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.randomCronSchedule
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.randomIntervalSchedule
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.snapshots.SnapshotId
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.snapshots.SnapshotState
import org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength
import org.opensearch.test.OpenSearchTestCase.randomIntBetween
import org.opensearch.test.OpenSearchTestCase.randomNonNegativeLong
import java.time.Instant
import java.time.Instant.now

fun randomSMMetadata(
    currentState: SMState,
    nextCreationTime: Instant = now(),
    nextDeletionTime: Instant = now(),
    policySeqNo: Long = randomNonNegativeLong(),
    policyPrimaryTerm: Long = randomNonNegativeLong(),
    startedCreation: SMMetadata.SnapshotInfo? = null,
    startedDeletion: List<SMMetadata.SnapshotInfo>? = null,
    deleteStartedTime: Instant? = null,
): SMMetadata {
    return SMMetadata(
        policySeqNo = policySeqNo,
        policyPrimaryTerm = policyPrimaryTerm,
        currentState = currentState,
        creation = SMMetadata.Creation(
            trigger = SMMetadata.Trigger(
                time = nextCreationTime
            ),
            started = startedCreation,
        ),
        deletion = SMMetadata.Deletion(
            trigger = SMMetadata.Trigger(
                time = nextDeletionTime
            ),
            started = startedDeletion,
            startedTime = deleteStartedTime,
        ),
    )
}

fun randomSMPolicy(
    id: String = randomAlphaOfLength(10),
    jobEnabled: Boolean = false,
    jobLastUpdateTime: Instant = randomInstant(),
    creationSchedule: CronSchedule = randomCronSchedule(),
    creationTimeLimit: TimeValue? = null,
    deletionSchedule: CronSchedule = randomCronSchedule(),
    deletionTimeLimit: TimeValue? = null,
    deletionMaxCount: Int = randomIntBetween(5, 10),
    deletionMaxAge: TimeValue? = null,
    deletionMinCount: Int? = null,
    snapshotConfig: Map<String, Any> = mapOf(
        "repository" to "repo",
        "date_format" to "yyyy-MM-dd-HH:mm"
    ),
    jobEnabledTime: Instant = randomInstant(),
    jobSchedule: IntervalSchedule = randomIntervalSchedule()
): SMPolicy {
    return SMPolicy(
        id = id,
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
        jobEnabledTime = jobEnabledTime,
        jobSchedule = jobSchedule
    )
}

fun randomSMSnapshotInfo(
    name: String = randomAlphaOfLength(10),
    startTime: Instant = randomInstant(),
) = SMMetadata.SnapshotInfo(
    name = name,
    startTime = startTime,
)

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

fun mockSnapshotInfo(
    name: String = randomAlphaOfLength(10),
    startTime: Long = randomNonNegativeLong(),
    endTime: Long = randomNonNegativeLong(),
    reason: String? = null, // reason with valid string leads to FAILED snapshot state
): SnapshotInfo {
    return SnapshotInfo(
        SnapshotId(name, UUIDs.randomBase64UUID()),
        listOf("index1"),
        listOf("ds-1"),
        startTime,
        reason,
        endTime,
        5,
        emptyList(),
        false,
        emptyMap(),
    )
}

fun mockInProgressSnapshotInfo(
    name: String = randomAlphaOfLength(10),
): SnapshotInfo {
    return SnapshotInfo(
        SnapshotId(name, UUIDs.randomBase64UUID()),
        listOf("index1"),
        listOf("ds-1"),
        SnapshotState.IN_PROGRESS,
    )
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
