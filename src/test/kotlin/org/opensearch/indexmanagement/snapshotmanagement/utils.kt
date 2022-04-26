/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.action.index.IndexResponse
import org.opensearch.indexmanagement.randomCronSchedule
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.randomIntervalSchedule
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength
import org.opensearch.test.OpenSearchTestCase.randomNonNegativeLong
import java.time.Instant
import java.time.Instant.now

fun getTestSMMetadata(
    currentState: SMState,
    nextCreationTime: Instant = now(),
    nextDeletionTime: Instant = now(),
    policySeqNo: Long = randomNonNegativeLong(),
    policyPrimaryTerm: Long = randomNonNegativeLong(),
    apiCalling: Boolean = false,
): SMMetadata {
    return SMMetadata(
        policySeqNo = policySeqNo,
        policyPrimaryTerm = policyPrimaryTerm,
        currentState = currentState.toString(),
        apiCalling = apiCalling,
        creation = SMMetadata.Creation(
            trigger = SMMetadata.Trigger(
                nextExecutionTime = nextCreationTime
            )
        ),
        deletion = SMMetadata.Deletion(
            trigger = SMMetadata.Trigger(
                nextExecutionTime = nextDeletionTime
            )
        ),
    )
}

fun getTestSMPolicy(
    policyName: String = randomAlphaOfLength(10),
    jobEnabled: Boolean = false,
    jobLastUpdateTime: Instant = randomInstant(),
    creationSchedule: CronSchedule = randomCronSchedule(),
    creationTimeout: String = "1h",
    deletionSchedule: CronSchedule = randomCronSchedule(),
    deletionTimeout: String = "1h",
    deletionConditionAge: String = "1h",
    deletionConditionCount: List<Int> = listOf(5, 10),
    snapshotConfig: Map<String, Any> = mapOf(
        "repository" to "repo",
        "date_format" to "yyyy-MM-dd-HH:mm"
    ),
    jobEnabledTime: Instant = randomInstant(),
    jobSchedule: IntervalSchedule = randomIntervalSchedule()
): SMPolicy {
    return SMPolicy(
        policyName = policyName,
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
                age = deletionConditionAge,
                count = deletionConditionCount
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
