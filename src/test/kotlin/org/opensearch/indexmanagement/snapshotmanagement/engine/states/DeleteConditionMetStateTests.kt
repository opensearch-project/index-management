/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.ClientMockTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.randomSMSnapshotInfo
import java.time.Instant

class DeleteConditionMetStateTests : ClientMockTestCase() {

    fun `test next deletion time met`() = runBlocking {
        val metadata = randomSMMetadata(
            currentState = SMState.START,
            nextDeletionTime = Instant.now().minusSeconds(60),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETE_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNotEquals("Next execution time should be updated.", metadata.deletion.trigger.time, result.metadataToSave.deletion.trigger.time)
    }

    fun `test next deletion time has not met`() = runBlocking {
        val metadata = randomSMMetadata(
            currentState = SMState.START,
            nextDeletionTime = Instant.now().plusSeconds(60),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETE_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertNull("Next execution time should not be updated.", result.metadataToSave)
    }

    fun `test already started snapshot deletion`() = runBlocking {
        val metadata = randomSMMetadata(
            currentState = SMState.START,
            startedDeletion = listOf(randomSMSnapshotInfo()),
            deleteStartedTime = randomInstant(),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETE_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Retry.", result is SMResult.Stay)
    }
}
