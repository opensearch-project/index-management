/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.ClientMockTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.randomSnapshotName
import java.time.Instant

class DeletionConditionMetStateTests : ClientMockTestCase() {

    fun `test next deletion time met`() = runBlocking {
        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_START,
            nextDeletionTime = Instant.now().minusSeconds(60),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETION_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNotEquals("Next execution time should be updated.", metadata.deletion!!.trigger.time, result.metadataToSave.build().deletion!!.trigger.time)
    }

    fun `test next deletion time has not met`() = runBlocking {
        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_START,
            nextDeletionTime = Instant.now().plusSeconds(60),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETION_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertEquals("Next execution time should not be updated.", metadata, result.metadataToSave.build())
    }

    fun `test already started snapshot deletion`() = runBlocking {
        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_START,
            startedDeletion = listOf(randomSnapshotName()),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETION_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertEquals("Metadata shouldn't be updated.", metadata, result.metadataToSave.build())
    }

    fun `test job deletion config is null`() = runBlocking {
        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_START,
            startedDeletion = listOf(randomSnapshotName()),
        )
        val job = randomSMPolicy(
            deletionNull = true
        )
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.DELETION_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertEquals("Metadata shouldn't be updated.", metadata, result.metadataToSave.build())
    }
}
