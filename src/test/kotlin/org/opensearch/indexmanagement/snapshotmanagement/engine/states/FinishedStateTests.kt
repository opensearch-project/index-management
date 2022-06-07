/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import kotlinx.coroutines.runBlocking
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.ClientMockTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockInProgressSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomLatestExecution
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import java.time.Instant.now

class FinishedStateTests : ClientMockTestCase() {

    fun `test creation end successful`() = runBlocking {
        val snapshotName = "test_creation_succeed"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = now(),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNull("Started creation should be reset to null.", result.metadataToSave.creation.started)
        assertEquals("Latest execution status is success", SMMetadata.LatestExecution.Status.SUCCESS, result.metadataToSave.creation.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", result.metadataToSave.creation.latestExecution!!.endTime)
        assertNotNull("Latest execution status message should not be null", result.metadataToSave.creation.latestExecution!!.info!!.message)
    }

    fun `test creation still in progress`() = runBlocking {
        val snapshotName = "test_creation_in_progress"
        val snapshotInfo = mockInProgressSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = now(),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertEquals("Started creation should not be reset.", snapshotName, result.metadataToSave.creation.started!!.first())
    }

    fun `test creation end not successful`() = runBlocking {
        val snapshotName = "test_creation_end_with_failed_state"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName, reason = "failed state")
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = now(),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNull("Started creation should be reset to null.", result.metadataToSave.creation.started)
        assertEquals("Latest execution status is failed", SMMetadata.LatestExecution.Status.FAILED, result.metadataToSave.creation.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", result.metadataToSave.creation.latestExecution!!.endTime)
        assertNotNull("Latest execution status cause should not be null", result.metadataToSave.creation.latestExecution!!.info!!.cause)
    }

    fun `test get snapshots exception in creation`() = runBlocking {
        val snapshotName = "test_creation_get_snapshots_exception"
        mockGetSnapshotsCall(exception = Exception())

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = now(),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        assertEquals("Latest execution status is retrying", SMMetadata.LatestExecution.Status.RETRYING, result.metadataToSave.creation.latestExecution!!.status)
        assertNull("Latest execution status end_time should be null", result.metadataToSave.creation.latestExecution!!.endTime)
        assertNotNull("Latest execution status info should not be null", result.metadataToSave.creation.latestExecution!!.info)
    }

    fun `test get snapshots empty in creation`() = runBlocking {
        val snapshotName = "test_creation_get_snapshots_empty"
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = now(),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertEquals("Latest execution status is success", SMMetadata.LatestExecution.Status.SUCCESS, result.metadataToSave.creation.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", result.metadataToSave.creation.latestExecution!!.endTime)
        assertNotNull("Latest execution status message should not be null", result.metadataToSave.creation.latestExecution!!.info!!.message)
    }

    fun `test creation time limit exceed`() = runBlocking {
        val snapshotName = "test_creation_time_exceed"
        val snapshotInfo = mockInProgressSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = now().minusSeconds(10),
            ),
        )
        val job = randomSMPolicy(
            policyName = "daily-snapshot",
            creationTimeLimit = TimeValue.timeValueSeconds(5)
        )
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        assertTrue(result.timeLimitExceed!!)
        assertEquals("Latest execution status is time limit exceed", SMMetadata.LatestExecution.Status.TIME_LIMIT_EXCEEDED, result.metadataToSave.creation.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", result.metadataToSave.creation.latestExecution!!.endTime)
        assertNotNull("Latest execution status cause should not be null", result.metadataToSave.creation.latestExecution!!.info!!.cause)
    }

    fun `test deletion succeed`() = runBlocking {
        val snapshotName = "test_deletion_success"
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            deletionLatestExecution = randomLatestExecution(
                startTime = now().minusSeconds(50),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNull("Started deletion should be reset to null.", result.metadataToSave.deletion.started)
        assertEquals("Latest execution status is success", SMMetadata.LatestExecution.Status.SUCCESS, result.metadataToSave.deletion.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", result.metadataToSave.deletion.latestExecution!!.endTime)
        assertNotNull("Latest execution status message should not be null", result.metadataToSave.deletion.latestExecution!!.info!!.message)
    }

    fun `test deletion has not finished`() = runBlocking {
        val snapshotName = "test_deletion_not_finished"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            deletionLatestExecution = randomLatestExecution(
                startTime = now().minusSeconds(50),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertNotNull("Started deletion should not be reset.", result.metadataToSave.deletion.started)
        assertEquals("Latest execution status is in_progress", SMMetadata.LatestExecution.Status.IN_PROGRESS, result.metadataToSave.deletion.latestExecution!!.status)
        assertNull("Latest execution status end_time should be null", result.metadataToSave.deletion.latestExecution!!.endTime)
    }

    fun `test get snapshots exception in deletion`() = runBlocking {
        val snapshotName = "test_deletion_get_snapshots_exception"
        mockGetSnapshotsCall(exception = Exception())

        val metadata = randomSMMetadata(
            currentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            deletionLatestExecution = randomLatestExecution(
                startTime = now().minusSeconds(50),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        assertEquals("Latest execution status is retrying", SMMetadata.LatestExecution.Status.RETRYING, result.metadataToSave.deletion.latestExecution!!.status)
        assertNull("Latest execution status end_time should be null", result.metadataToSave.deletion.latestExecution!!.endTime)
        assertNotNull("Latest execution status info should not be null", result.metadataToSave.deletion.latestExecution!!.info)
    }

    fun `test deletion time limit exceed`() = runBlocking {
        val snapshotName = "test_deletion_time_exceed"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            deletionLatestExecution = randomLatestExecution(
                startTime = now().minusSeconds(50),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot", deletionTimeLimit = TimeValue.timeValueSeconds(5))
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        assertTrue(result.timeLimitExceed!!)
        assertEquals("Latest execution status is time limit exceed", SMMetadata.LatestExecution.Status.TIME_LIMIT_EXCEEDED, result.metadataToSave.deletion.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", result.metadataToSave.deletion.latestExecution!!.endTime)
        assertNotNull("Latest execution status cause should not be null", result.metadataToSave.deletion.latestExecution!!.info!!.cause)
    }
}
