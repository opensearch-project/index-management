/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.spy
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import kotlinx.coroutines.runBlocking
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.ClientMockTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.smTransitions
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import java.time.Instant.now

class SMStateMachineTests : ClientMockTestCase() {

    fun `test sm result Next save the current state`() = runBlocking {
        val currentState = SMState.FINISHED
        val nextStates = smTransitions[currentState]
        val metadata = randomSMMetadata(
            currentState = currentState
        )
        val job = randomSMPolicy()
        val stateMachineSpy = spy(SMStateMachine(client, job, metadata))

        stateMachineSpy.next(smTransitions)
        argumentCaptor<SMMetadata>().apply {
            verify(stateMachineSpy).updateMetadata(capture())
            assertEquals(nextStates!!.first(), firstValue.currentState)
        }
    }

    fun `test sm result Stay save the previous state`() = runBlocking {
        val currentState = SMState.START
        // both creation and deletion conditions are not met
        val metadata = randomSMMetadata(
            currentState = currentState,
            nextCreationTime = now().plusSeconds(60),
            nextDeletionTime = now().plusSeconds(60),
        )
        val job = randomSMPolicy()
        val stateMachineSpy = spy(SMStateMachine(client, job, metadata))

        stateMachineSpy.next(smTransitions)
        argumentCaptor<SMMetadata>().apply {
            verify(stateMachineSpy, times(2)).updateMetadata(capture())
            assertEquals(currentState, firstValue.currentState)
            assertEquals(currentState, secondValue.currentState)
        }
    }

    fun `test sm result Failure reset workflow and add info`() = runBlocking {
        val currentState = SMState.CREATE_CONDITION_MET
        val resetState = SMState.CREATING
        val ex = Exception()
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))
        mockCreateSnapshotCall(exception = ex)

        val metadata = randomSMMetadata(
            currentState = currentState,
        )
        val job = randomSMPolicy()
        val stateMachineSpy = spy(SMStateMachine(client, job, metadata))
        stateMachineSpy.next(smTransitions)
        argumentCaptor<SMMetadata>().apply {
            verify(stateMachineSpy).updateMetadata(capture())
            assertEquals(resetState, firstValue.currentState)
            assertNull(firstValue.creation.started)
            assertTrue(firstValue.info!!.contains("exception"))
        }
    }

    fun `test sm result TimeLimitExceed reset workflow`() = runBlocking {
        val currentState = SMState.CREATING
        val resetState = SMState.DELETING

        // deletion time limit exceed
        val snapshotName = "test_state_machine_deletion_time_exceed"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))
        val metadata = randomSMMetadata(
            currentState = currentState,
            startedDeletion = listOf(
                SMMetadata.SnapshotInfo(
                    name = snapshotName,
                    startTime = now().minusSeconds(100),
                ),
            ),
            deleteStartedTime = now().minusSeconds(50),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot", deletionTimeLimit = TimeValue.timeValueSeconds(5))

        val stateMachineSpy = spy(SMStateMachine(client, job, metadata))
        stateMachineSpy.next(smTransitions)
        argumentCaptor<SMMetadata>().apply {
            verify(stateMachineSpy, times(2)).updateMetadata(capture())
            // first try DELETE_CONDITION_MET state and Stay
            assertEquals(currentState, firstValue.currentState)
            assertEquals(resetState, secondValue.currentState)
            assertNull(secondValue.deletion.started)
            assertNull(secondValue.deletion.startedTime)
        }
    }

    fun `test sm result Retry retry count initialized`() = runBlocking {
        val currentState = SMState.CREATE_CONDITION_MET

        val ex = Exception()
        mockGetSnapshotsCall(exception = ex)
        val metadata = randomSMMetadata(
            currentState = currentState,
        )
        val job = randomSMPolicy()

        val stateMachineSpy = spy(SMStateMachine(client, job, metadata))
        stateMachineSpy.next(smTransitions)
        argumentCaptor<SMMetadata>().apply {
            verify(stateMachineSpy).updateMetadata(capture())
            assertEquals(currentState, firstValue.currentState)
            assertEquals(3, firstValue.creation.retry!!.count)
        }
    }

    fun `test sm result Retry retry count has exhausted and reset workflow`() = runBlocking {
        val currentState = SMState.DELETE_CONDITION_MET
        val resetState = SMState.DELETING

        val ex = Exception()
        mockGetSnapshotsCall(exception = ex)
        val metadata = randomSMMetadata(
            currentState = currentState,
            deletionRetryCount = 1,
        )
        val job = randomSMPolicy()

        val stateMachineSpy = spy(SMStateMachine(client, job, metadata))
        stateMachineSpy.next(smTransitions)
        argumentCaptor<SMMetadata>().apply {
            verify(stateMachineSpy).updateMetadata(capture())
            assertEquals(resetState, firstValue.currentState)
            assertNull(firstValue.deletion.retry)
            assertNull(firstValue.deletion.started)
            assertNull(firstValue.deletion.startedTime)
        }
    }

    fun `test handlePolicyChange`() = runBlocking {
        val metadata = randomSMMetadata(
            policySeqNo = 0,
            policyPrimaryTerm = 0,
        )
        val job = randomSMPolicy(
            seqNo = 1,
            primaryTerm = 1,
        )

        val stateMachineSpy = spy(SMStateMachine(client, job, metadata))
        stateMachineSpy.handlePolicyChange()
        argumentCaptor<SMMetadata>().apply {
            verify(stateMachineSpy).updateMetadata(capture())
            assertEquals(1, firstValue.policySeqNo)
            assertEquals(1, firstValue.policyPrimaryTerm)
        }
    }
}
