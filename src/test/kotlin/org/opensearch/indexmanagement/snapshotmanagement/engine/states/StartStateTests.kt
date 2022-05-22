/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import com.nhaarman.mockitokotlin2.mock
import kotlinx.coroutines.runBlocking
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.test.OpenSearchTestCase

class StartStateTests : OpenSearchTestCase() {

    private val client: Client = mock()

    fun `test start state execution`() = runBlocking {
        val metadata = randomSMMetadata(
            currentState = SMState.FINISHED
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val result = SMState.START.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
    }
}
