/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.ClientMockTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
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
    fun `test creation in progress`() = runBlocking { }
    fun `test creation not successful`() = runBlocking { }
    fun `test deletion succeed`() = runBlocking { }
    fun `test deletion in progress`() = runBlocking { }
}
