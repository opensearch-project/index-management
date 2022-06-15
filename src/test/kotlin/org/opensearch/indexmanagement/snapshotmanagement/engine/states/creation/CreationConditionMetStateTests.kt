/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation

import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.ClientMockTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.randomSnapshotName
import java.time.Instant.now

class CreationConditionMetStateTests : ClientMockTestCase() {

    fun `test next creation time met`() = runBlocking {
        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATION_START,
            nextCreationTime = now().minusSeconds(60),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.CREATION_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        assertNotEquals("Next execution time should be updated.", metadata.creation.trigger.time, result.metadataToSave.build().creation.trigger.time)
    }

    fun `test next creation time has not met`() = runBlocking {
        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATION_START,
            nextCreationTime = now().plusSeconds(60),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.CREATION_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertEquals("Next execution time should not be updated.", metadata, result.metadataToSave.build())
    }

    fun `test already started snapshot creation`() = runBlocking {
        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATION_START,
            startedCreation = randomSnapshotName(),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.CREATION_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        assertEquals("Metadata shouldn't be updated.", metadata, result.metadataToSave.build())
    }
}