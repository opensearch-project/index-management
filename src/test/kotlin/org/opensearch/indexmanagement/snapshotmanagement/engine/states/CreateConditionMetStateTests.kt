/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import com.nhaarman.mockitokotlin2.mock
import kotlinx.coroutines.runBlocking
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.getTestSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.getTestSMPolicy
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant.now

class CreateConditionMetStateTests : OpenSearchTestCase() {

    private val client: Client = mock()

    fun `test next creation time met`() = runBlocking {
        val metadata = getTestSMMetadata(
            currentState = SMState.START,
            nextCreationTime = now().minusSeconds(5),
        )
        val job = getTestSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val end = SMState.CREATE_CONDITION_MET.instance.execute(context)
        assertEquals("Execution should return true.", true, end)
        assertEquals("Current state should move to CREATE_CONDITION_MET.", SMState.CREATE_CONDITION_MET.toString(), context.metadataToSave!!.currentState)
        assertNotEquals("Next execution time should be updated.", metadata.creation.trigger.nextExecutionTime, context.metadataToSave!!.creation.trigger.nextExecutionTime)
    }
}
