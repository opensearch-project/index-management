/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import kotlinx.coroutines.runBlocking
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementClientMockTests
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy

class DeletingStateTests : SnapshotManagementClientMockTests() {

    fun `test delete snapshots succeed`() = runBlocking {
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(11))
        mockDeleteSnapshotCall(response = AcknowledgedResponse(true))

        val metadata = randomSMMetadata(
            currentState = SMState.DELETE_CONDITION_MET,
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNotNull("Deletion started field is initialized.", result.metadataToSave.deletion.started)
    }

    fun `test delete snapshot exception`() = runBlocking {
        val ex = Exception()
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(11))
        mockDeleteSnapshotCall(exception = ex)

        val metadata = randomSMMetadata(
            currentState = SMState.DELETE_CONDITION_MET,
        )
        val job = randomSMPolicy()
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
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Retry)
    }

    // TODO SM test findSnapshotsToDelete
}
