/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.ClientMockTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockInProgressSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
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
            startedCreation = SMMetadata.SnapshotInfo(
                name = snapshotName,
                startTime = now(),
            )
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNull("Started creation should be reset to null.", result.metadataToSave.creation.started)
        assertEquals("Info last_success should be set.", snapshotName, result.metadataToSave.info!!["last_success"])
    }

    fun `test creation in progress`() = runBlocking {
        val snapshotName = "test_creation_in_progress"
        val snapshotInfo = mockInProgressSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.CREATING,
            startedCreation = SMMetadata.SnapshotInfo(
                name = snapshotName,
                startTime = now(),
            )
        )
        val job = randomSMPolicy()
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
            startedCreation = SMMetadata.SnapshotInfo(
                name = snapshotName,
                startTime = now(),
            )
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNull("Started creation should be reset to null.", result.metadataToSave.creation.started)
    }

    fun `test deletion succeed`() = runBlocking {
        val snapshotName = "test_deletion_success"
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETING,
            startedDeletion = listOf(
                SMMetadata.SnapshotInfo(
                    name = snapshotName,
                    startTime = now().minusSeconds(100)
                ),
            ),
            deleteStartedTime = now().minusSeconds(50),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNull("Started deletion should be reset to null.", result.metadataToSave.deletion.started)
        assertNull("Deletion started time should be reset to null.", result.metadataToSave.deletion.startedTime)
    }

    fun `test deletion has not finished`() = runBlocking {
        val snapshotName = "test_deletion_not_finished"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETING,
            startedDeletion = listOf(
                SMMetadata.SnapshotInfo(
                    name = snapshotName,
                    startTime = now().minusSeconds(100),
                ),
            ),
            deleteStartedTime = now().minusSeconds(50),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.FINISHED.instance.execute(context)
        assertTrue("Execution results should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertNotNull("Started deletion should not be reset.", result.metadataToSave!!.deletion.started)
        assertNotNull("Started deletion time should not be reset.", result.metadataToSave!!.deletion.startedTime)
    }
}
