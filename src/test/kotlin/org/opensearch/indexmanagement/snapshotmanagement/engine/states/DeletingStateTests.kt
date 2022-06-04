/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import kotlinx.coroutines.runBlocking
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.ClientMockTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import java.time.Instant.now

class DeletingStateTests : ClientMockTestCase() {

    fun `test snapshots exceed max count`() = runBlocking {
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(11))
        mockDeleteSnapshotCall(response = AcknowledgedResponse(true))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETE_CONDITION_MET,
        )
        val job = randomSMPolicy(
            policyName = "daily-snapshot",
            deletionMaxCount = 10,
        )
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNotNull("Deletion started field is initialized.", result.metadataToSave.deletion.started)
        assertEquals(1, result.metadataToSave.deletion.started!!.size)
    }

    fun `test snapshots exceed max age`() = runBlocking {
        val oldSnapshot = mockSnapshotInfo(name = "old_snapshot", startTime = now().minusSeconds(2 * 60).toEpochMilli())
        val newSnapshot = mockSnapshotInfo(name = "new_snapshot", startTime = now().toEpochMilli())
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(listOf(oldSnapshot, newSnapshot, newSnapshot)))
        mockDeleteSnapshotCall(response = AcknowledgedResponse(true))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETE_CONDITION_MET,
        )
        val job = randomSMPolicy(
            policyName = "daily-snapshot",
            deletionMaxAge = TimeValue.timeValueMinutes(1),
            deletionMinCount = 2,
        )
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNotNull("Deletion started field is initialized.", result.metadataToSave.deletion.started)
        assertEquals(1, result.metadataToSave.deletion.started!!.size)
        assertEquals("old_snapshot", result.metadataToSave.deletion.started!!.first())
    }

    fun `test snapshots exceed max age but need to remain min count`() = runBlocking {
        val oldSnapshot = mockSnapshotInfo(name = "old_snapshot", startTime = now().minusSeconds(2 * 60).toEpochMilli())
        val newSnapshot = mockSnapshotInfo(name = "new_snapshot", startTime = now().toEpochMilli())
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(listOf(oldSnapshot, newSnapshot)))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETE_CONDITION_MET,
        )
        val job = randomSMPolicy(
            policyName = "daily-snapshot",
            deletionMaxAge = TimeValue.timeValueMinutes(1),
            deletionMinCount = 2,
        )
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNull("Deletion started field should not be initialized.", result.metadataToSave.deletion.started)
    }

    fun `test delete snapshot exception`() = runBlocking {
        val ex = Exception()
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(11))
        mockDeleteSnapshotCall(exception = ex)

        val metadata = randomSMMetadata(
            currentState = SMState.DELETE_CONDITION_MET,
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Failure.", result is SMResult.Failure)
        result as SMResult.Failure
        assertTrue("Delete snapshot exception should notify user.", result.notifiable)
    }

    fun `test get snapshots exception`() = runBlocking {
        val ex = Exception()
        mockGetSnapshotsCall(exception = ex)

        val metadata = randomSMMetadata(
            currentState = SMState.DELETE_CONDITION_MET,
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Retry)
    }
}
