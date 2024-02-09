/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation

import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.MocksTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy

class CreationStartStateTests : MocksTestCase() {

    fun `test start state execution`() = runBlocking {
        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATION_FINISHED,
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.CREATION_START.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
    }
}
