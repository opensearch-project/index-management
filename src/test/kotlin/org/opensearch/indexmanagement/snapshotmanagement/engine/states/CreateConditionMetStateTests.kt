/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import com.nhaarman.mockitokotlin2.mock
import kotlinx.coroutines.runBlocking
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.Result
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant.now

class CreateConditionMetStateTests : OpenSearchTestCase() {

    private val client: Client = mock()

    fun `test next creation time met`() = runBlocking {
        val metadata = randomSMMetadata(
            currentState = SMState.START,
            nextCreationTime = now().minusSeconds(5),
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val end = SMState.CREATE_CONDITION_MET.instance.execute(context)
        assertTrue("Execution result should be Next.", end is Result.Next)
        end as Result.Next
        assertEquals("Current state should move to CREATE_CONDITION_MET.", SMState.CREATE_CONDITION_MET, end.metadataToSave.currentState)
        assertNotEquals("Next execution time should be updated.", metadata.creation.trigger.time, end.metadataToSave.creation.trigger.time)
    }
}
