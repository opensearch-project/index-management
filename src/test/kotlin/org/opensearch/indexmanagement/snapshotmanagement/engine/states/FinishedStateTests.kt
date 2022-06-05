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
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import java.time.Instant.now

class FinishedStateTests : ClientMockTestCase() {

    fun `test creation succeed`() = runBlocking {
        val snapshotName = "test_creation_succeed"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            startedCreationTime = now(),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNull("Started creation should be reset to null.", result.metadataToSave.creation.started)
    }

    fun `test creation in progress`() = runBlocking {
        val snapshotName = "test_creation_in_progress"
        val snapshotInfo = mockInProgressSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            startedCreationTime = now(),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertNotNull("Started creation should not be reset.", result.metadataToSave!!.creation.started)
    }

    fun `test creation not successful`() = runBlocking {
        val snapshotName = "test_creation_end_with_failed_state"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName, reason = "failed state")
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            startedCreationTime = now(),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNull("Started creation should be reset to null.", result.metadataToSave.creation.started)
    }

    fun `test get snapshots exception in creation`() = runBlocking {
        val snapshotName = "test_creation_get_snapshots_exception"
        mockGetSnapshotsCall(exception = Exception())

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            startedCreationTime = now(),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Failure.", result is SMResult.Failure)
    }

    fun `test creation time limit exceed`() = runBlocking {
        val snapshotName = "test_creation_time_exceed"
        val snapshotInfo = mockInProgressSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = snapshotName,
            startedCreationTime = now().minusSeconds(10),
        )
        val job = randomSMPolicy(
            policyName = "daily-snapshot",
            creationTimeLimit = TimeValue.timeValueSeconds(5)
        )
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be TimeLimitExceed.", result is SMResult.TimeLimitExceed)
    }

    fun `test deletion succeed`() = runBlocking {
        val snapshotName = "test_deletion_success"
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            startedDeletionTime = now().minusSeconds(50),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNull("Started deletion should be reset to null.", result.metadataToSave.deletion.started)
    }

    fun `test deletion has not finished`() = runBlocking {
        val snapshotName = "test_deletion_not_finished"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            startedDeletionTime = now().minusSeconds(50),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertNotNull("Started deletion should not be reset.", result.metadataToSave.deletion.started)
    }

    fun `test get snapshots exception in deletion`() = runBlocking {
        val snapshotName = "test_deletion_get_snapshots_exception"
        mockGetSnapshotsCall(exception = Exception())

        val metadata = randomSMMetadata(
            currentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            startedDeletionTime = now().minusSeconds(50),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Failure.", result is SMResult.Failure)
    }

    fun `test deletion time limit exceed`() = runBlocking {
        val snapshotName = "test_deletion_time_exceed"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            startedDeletionTime = now().minusSeconds(50),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot", deletionTimeLimit = TimeValue.timeValueSeconds(5))
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be TimeLimitExceed.", result is SMResult.TimeLimitExceed)
    }
}
