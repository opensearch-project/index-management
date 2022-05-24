/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.spy
import com.nhaarman.mockitokotlin2.verify
import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.ClientMockTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.smTransitions
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy

class SMStateMachineTests : ClientMockTestCase() {

    fun `test basic`() = runBlocking {
        val metadata = randomSMMetadata(
            currentState = SMState.FINISHED
        )
        val job = randomSMPolicy()
        val stateMachineSpy = spy(SMStateMachine(client, job, metadata))
        stateMachineSpy.next(smTransitions)
        argumentCaptor<SMMetadata>().apply {
            verify(stateMachineSpy).updateMetadata(capture())
            assertEquals(SMState.START, firstValue.currentState)
        }
    }
}
