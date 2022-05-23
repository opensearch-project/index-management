/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementClientMockTests
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.mockCreateSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import java.time.Instant.now

class CreatingStateTests : SnapshotManagementClientMockTests() {

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
        assertTrue("Execution result should be Failure.", result is SMResult.Failure)
        result as SMResult.Failure
        assertTrue("Create snapshot exception should notify user.", result.notifiable)
    }

    fun `test snapshot already created in previous schedule`() = runBlocking {
        val mockGetSnapshotResponse = mockGetSnapshotResponse(1, now().minusSeconds(30).toEpochMilli())
        val snapshotName = mockGetSnapshotResponse.snapshots.first().snapshotId().name
        mockGetSnapshotsCall(response = mockGetSnapshotResponse)

        val metadata = randomSMMetadata(
            currentState = SMState.CREATE_CONDITION_MET,
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.CREATING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertEquals("Started create snapshot name is $snapshotName.", snapshotName, result.metadataToSave.creation.started!!.name)
    }
}
