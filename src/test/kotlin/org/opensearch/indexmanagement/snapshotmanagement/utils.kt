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
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.snapshots.SnapshotId
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.test.OpenSearchTestCase.randomNonNegativeLong
import java.time.Instant
import java.time.Instant.now

fun randomSMMetadata(
    currentState: SMState,
    nextCreationTime: Instant = now(),
    nextDeletionTime: Instant = now(),
    policySeqNo: Long = randomNonNegativeLong(),
    policyPrimaryTerm: Long = randomNonNegativeLong(),
): SMMetadata {
    return SMMetadata(
        policySeqNo = policySeqNo,
        policyPrimaryTerm = policyPrimaryTerm,
        currentState = currentState,
        creation = SMMetadata.Creation(
            trigger = SMMetadata.Trigger(
                time = nextCreationTime
            )
        ),
        deletion = SMMetadata.Deletion(
            trigger = SMMetadata.Trigger(
                time = nextDeletionTime
            )
        ),
    )
}

fun randomSMPolicy(
    jobEnabled: Boolean = false,
    jobLastUpdateTime: Instant = randomInstant(),
    creationSchedule: CronSchedule = randomCronSchedule(),
    creationTimeout: TimeValue? = null,
    deletionSchedule: CronSchedule = randomCronSchedule(),
    deletionTimeout: TimeValue? = null,
    deletionMaxCount: Int = 5,
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
        jobEnabled = jobEnabled,
        jobLastUpdateTime = jobLastUpdateTime,
        creation = SMPolicy.Creation(
            schedule = creationSchedule,
            timeout = creationTimeout,
        ),
        deletion = SMPolicy.Deletion(
            schedule = deletionSchedule,
            timeout = deletionTimeout,
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

fun mockGetSnapshotResponse(num: Int): GetSnapshotsResponse {
    val getSnapshotsRes: GetSnapshotsResponse = mock()
    whenever(getSnapshotsRes.snapshots).doReturn(mockSnapshots(num))
    return getSnapshotsRes
}

fun mockSnapshots(num: Int): List<SnapshotInfo> {
    val result = mutableListOf<SnapshotInfo>()
    for (i in 1..num) {
        result.add(mockSnapshotInfo(idNum = i))
    }
    return result.toList()
}

/**
 * For our use case, only mock snapshotId, startTime and endTime
 */
fun mockSnapshotInfo(idNum: Int, startTime: Long = randomNonNegativeLong(), endTime: Long = randomNonNegativeLong()): SnapshotInfo {
    val snapshotId = SnapshotId("mock_snapshot-$idNum}", UUIDs.randomBase64UUID())
    return SnapshotInfo(
        snapshotId,
        listOf("index1"),
        listOf("ds-1"),
        startTime,
        "",
        endTime,
        5,
        emptyList(),
        false,
        emptyMap(),
    )
}
