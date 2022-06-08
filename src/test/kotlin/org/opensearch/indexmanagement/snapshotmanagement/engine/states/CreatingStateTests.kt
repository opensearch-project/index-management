/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.ClientMockTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.mockCreateSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import java.time.Instant.now
import java.time.temporal.ChronoUnit

class CreatingStateTests : ClientMockTestCase() {

    fun `test create snapshot succeed`() = runBlocking {
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))
        mockCreateSnapshotCall(response = mockCreateSnapshotResponse())

        val metadata = randomSMMetadata(
            currentState = SMState.CREATE_CONDITION_MET,
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.CREATING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNotNull("Creation started field is initialized.", result.metadataToSave.creation.started)
        assertEquals("Latest execution status is in_progress", SMMetadata.LatestExecution.Status.IN_PROGRESS, result.metadataToSave.creation.latestExecution!!.status)
    }

    fun `test create snapshot exception`() = runBlocking {
        val ex = Exception()
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))
        mockCreateSnapshotCall(exception = ex)

        val metadata = randomSMMetadata(
            currentState = SMState.CREATE_CONDITION_MET,
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.CREATING.instance.execute(context)
        assertTrue("Execution result should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        assertNull("Creation started field should not be initialized.", result.metadataToSave.creation.started)
        assertEquals("Latest execution status is retrying", SMMetadata.LatestExecution.Status.RETRYING, result.metadataToSave.creation.latestExecution!!.status)
        assertNotNull("Latest execution info should not be null", result.metadataToSave.creation.latestExecution!!.info)
    }

    fun `test snapshot already created in previous schedule`() = runBlocking {
        val mockSnapshotInfo = mockSnapshotInfo(startTime = now().minusMillis(30).toEpochMilli())
        val mockGetSnapshotResponse = mockGetSnapshotResponse(mockSnapshotInfo)
        val snapshotName = mockGetSnapshotResponse.snapshots.first().snapshotId().name
        mockGetSnapshotsCall(response = mockGetSnapshotResponse)

        val metadata = randomSMMetadata(
            currentState = SMState.CREATE_CONDITION_MET,
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.CREATING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertEquals("Started create snapshot name is $snapshotName.", snapshotName, result.metadataToSave.creation.started!!.first())
        assertEquals("Latest execution status is in_progress", SMMetadata.LatestExecution.Status.IN_PROGRESS, result.metadataToSave.creation.latestExecution!!.status)
    }

    fun `test snapshot already created but not in previous schedule`() = runBlocking {
        val mockSnapshotInfo = mockSnapshotInfo(startTime = now().minus(370, ChronoUnit.DAYS).toEpochMilli())
        val mockGetSnapshotResponse = mockGetSnapshotResponse(mockSnapshotInfo)
        val snapshotName = mockGetSnapshotResponse.snapshots.first().snapshotId().name
        mockGetSnapshotsCall(response = mockGetSnapshotResponse)
        mockCreateSnapshotCall(response = mockCreateSnapshotResponse())

        val metadata = randomSMMetadata(
            currentState = SMState.CREATE_CONDITION_MET,
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.CREATING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNotEquals("Started create snapshot name should not be $snapshotName.", snapshotName, result.metadataToSave.creation.started!!.first())
    }

    fun `test get snapshots exception while checking if snapshot already created`() = runBlocking {
        val ex = Exception()
        mockGetSnapshotsCall(exception = ex)

        val metadata = randomSMMetadata(
            currentState = SMState.CREATE_CONDITION_MET,
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.CREATING.instance.execute(context)
        assertTrue("Execution result should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        assertNull("Creation started field should not be initialized.", result.metadataToSave.creation.started)
        assertEquals("Latest execution status is retrying", SMMetadata.LatestExecution.Status.RETRYING, result.metadataToSave.creation.latestExecution!!.status)
        assertNotNull("Latest execution info should not be null", result.metadataToSave.creation.latestExecution!!.info)
    }
}
