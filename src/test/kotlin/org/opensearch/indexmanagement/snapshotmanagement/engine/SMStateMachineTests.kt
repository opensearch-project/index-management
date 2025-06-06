/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.spy
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.OpenSearchException
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.indexmanagement.MocksTestCase
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementException
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.creationTransitions
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletionTransitions
import org.opensearch.indexmanagement.snapshotmanagement.mockCreateSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomLatestExecution
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import java.time.Instant.now

open class SMStateMachineTests : MocksTestCase() {
    fun `test sm result Next save the current state`() =
        runBlocking {
            val currentState = SMState.CREATION_CONDITION_MET
            val nextStates = creationTransitions[currentState]
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))
            mockCreateSnapshotCall(response = mockCreateSnapshotResponse())

            val metadata =
                randomSMMetadata(
                    creationCurrentState = currentState,
                )
            val job = randomSMPolicy()
            val stateMachineSpy = spy(SMStateMachine(client, job, metadata, settings, threadPool, indicesManager))

            stateMachineSpy.currentState(currentState).next(creationTransitions)
            argumentCaptor<SMMetadata>().apply {
                verify(stateMachineSpy).updateMetadata(capture())
                assertEquals(nextStates!!.first(), firstValue.creation.currentState)
            }
        }

    fun `test sm result Stay save the previous state`() =
        runBlocking {
            val currentState = SMState.DELETION_START
            // both creation and deletion conditions are not met
            val metadata =
                randomSMMetadata(
                    deletionCurrentState = currentState,
                    nextCreationTime = now().plusSeconds(60),
                    nextDeletionTime = now().plusSeconds(60),
                )
            val job = randomSMPolicy()
            val stateMachineSpy = spy(SMStateMachine(client, job, metadata, settings, threadPool, indicesManager))

            stateMachineSpy.currentState(currentState).next(deletionTransitions)
            argumentCaptor<SMMetadata>().apply {
                verify(stateMachineSpy, times(1)).updateMetadata(capture())
                assertEquals(currentState, firstValue.deletion!!.currentState)
            }
        }

    fun `test sm result Fail starts retry for creation workflow`() =
        runBlocking {
            val currentState = SMState.CREATION_CONDITION_MET
            val ex = Exception()
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))
            mockCreateSnapshotCall(exception = ex)

            val metadata =
                randomSMMetadata(
                    creationCurrentState = currentState,
                    creationLatestExecution =
                    randomLatestExecution(
                        status = SMMetadata.LatestExecution.Status.RETRYING,
                    ),
                )
            val job = randomSMPolicy()
            val stateMachineSpy = spy(SMStateMachine(client, job, metadata, settings, threadPool, indicesManager))
            stateMachineSpy.currentState(currentState).next(creationTransitions)
            argumentCaptor<SMMetadata>().apply {
                verify(stateMachineSpy).updateMetadata(capture())
                assertEquals(currentState, firstValue.creation.currentState)
                assertNull(firstValue.creation.started)
                assertEquals(3, firstValue.creation.retry!!.count)
            }
        }

    fun `test sm result Fail starts retry for deletion workflow`() =
        runBlocking {
            val currentState = SMState.DELETION_CONDITION_MET
            val ex = Exception()
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(11))
            mockDeleteSnapshotCall(exception = ex)

            val metadata =
                randomSMMetadata(
                    deletionCurrentState = currentState,
                    deletionLatestExecution =
                    randomLatestExecution(
                        status = SMMetadata.LatestExecution.Status.RETRYING,
                    ),
                )
            val job =
                randomSMPolicy(
                    policyName = "daily-snapshot",
                    deletionMaxCount = 10,
                )
            val stateMachineSpy = spy(SMStateMachine(client, job, metadata, settings, threadPool, indicesManager))
            stateMachineSpy.currentState(currentState).next(deletionTransitions)
            argumentCaptor<SMMetadata>().apply {
                verify(stateMachineSpy).updateMetadata(capture())
                assertEquals(currentState, firstValue.deletion!!.currentState)
                assertNull(firstValue.deletion!!.started)
                assertEquals(3, firstValue.deletion!!.retry!!.count)
            }
        }

    fun `test sm result Fail retry count remaining 2`() =
        runBlocking {
            val currentState = SMState.DELETION_CONDITION_MET

            val ex = Exception()
            mockGetSnapshotsCall(exception = ex)
            val metadata =
                randomSMMetadata(
                    deletionCurrentState = currentState,
                    deletionRetryCount = 2,
                    deletionLatestExecution =
                    randomLatestExecution(
                        status = SMMetadata.LatestExecution.Status.RETRYING,
                    ),
                )
            val job = randomSMPolicy()

            val stateMachineSpy = spy(SMStateMachine(client, job, metadata, settings, threadPool, indicesManager))
            stateMachineSpy.currentState(currentState).next(deletionTransitions)
            argumentCaptor<SMMetadata>().apply {
                verify(stateMachineSpy).updateMetadata(capture())
                assertEquals(currentState, firstValue.deletion!!.currentState)
                assertEquals(1, firstValue.deletion?.retry!!.count)
            }
        }

    fun `test sm result Fail retry count has exhausted and reset workflow`() =
        runBlocking {
            val currentState = SMState.DELETION_CONDITION_MET
            val resetState = SMState.DELETION_START

            val ex = Exception()
            mockGetSnapshotsCall(exception = ex)
            val metadata =
                randomSMMetadata(
                    deletionCurrentState = currentState,
                    deletionRetryCount = 1,
                    deletionLatestExecution =
                    randomLatestExecution(
                        status = SMMetadata.LatestExecution.Status.RETRYING,
                    ),
                )
            val job = randomSMPolicy()

            val stateMachineSpy = spy(SMStateMachine(client, job, metadata, settings, threadPool, indicesManager))
            stateMachineSpy.currentState(currentState).next(deletionTransitions)
            argumentCaptor<SMMetadata>().apply {
                verify(stateMachineSpy).updateMetadata(capture())
                assertEquals(resetState, firstValue.deletion!!.currentState)
                assertNull(firstValue.deletion!!.retry)
                assertNull(firstValue.deletion!!.started)
            }
        }

    fun `test sm result Fail time limit exceed reset workflow`() =
        runBlocking {
            val currentState = SMState.DELETING
            val resetState = SMState.DELETION_START

            val snapshotName = "test_state_machine_deletion_time_exceed"
            val snapshotInfo = mockSnapshotInfo(name = snapshotName)
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))
            val metadata =
                randomSMMetadata(
                    deletionCurrentState = currentState,
                    startedDeletion = listOf(snapshotName),
                    deletionLatestExecution =
                    randomLatestExecution(
                        startTime = now().minusSeconds(50),
                    ),
                )
            val job =
                randomSMPolicy(
                    policyName = "daily-snapshot",
                    deletionTimeLimit = TimeValue.timeValueSeconds(5),
                )

            val stateMachineSpy = spy(SMStateMachine(client, job, metadata, settings, threadPool, indicesManager))
            stateMachineSpy.currentState(currentState).next(deletionTransitions)
            argumentCaptor<SMMetadata>().apply {
                // first execute DELETE_CONDITION_MET state and return Stay
                // second execute FINISHED state and return Fail because of deletion time_limit_exceed
                verify(stateMachineSpy, times(1)).updateMetadata(capture())
                assertEquals(resetState, firstValue.deletion!!.currentState)
            }
        }

    fun `test handlePolicyChange`() =
        runBlocking {
            val metadata =
                randomSMMetadata(
                    policySeqNo = 0,
                    policyPrimaryTerm = 0,
                )
            val job =
                randomSMPolicy(
                    seqNo = 1,
                    primaryTerm = 1,
                )

            val stateMachineSpy = spy(SMStateMachine(client, job, metadata, settings, threadPool, indicesManager))
            stateMachineSpy.handlePolicyChange()
            argumentCaptor<SMMetadata>().apply {
                verify(stateMachineSpy).updateMetadata(capture())
                assertEquals(1, firstValue.policySeqNo)
                assertEquals(1, firstValue.policyPrimaryTerm)
            }
        }

    fun `test updateMetadata handles VersionConflictEngineException gracefully`() = runBlocking {
        val initialMetadata = randomSMMetadata(
            policySeqNo = 0,
            policyPrimaryTerm = 0,
        )
        val smPolicy = randomSMPolicy(
            seqNo = 1,
            primaryTerm = 1,
        )
        val updatedMetadata = randomSMMetadata(
            policySeqNo = 1,
            policyPrimaryTerm = 1,
        )

        doAnswer {
            val listener = it.getArgument<ActionListener<ActionResponse>>(1)
            listener.onFailure(VersionConflictEngineException(ShardId("index", "_na_", 1), "test", "message"))
        }.whenever(client).index(any(), any())

        val stateMachineSpy = spy(SMStateMachine(client, smPolicy, initialMetadata, settings, threadPool, indicesManager))

        // Verify VersionConflictEngineException is handled gracefully
        try {
            stateMachineSpy.updateMetadata(updatedMetadata)
        } catch (e: Exception) {
            fail("VersionConflictEngineException should be handled without throwing: ${e.message}")
        }
    }

    fun `test updateMetadata throws SnapshotManagementException for other exceptions`() = runBlocking {
        val initialMetadata = randomSMMetadata(
            policySeqNo = 0,
            policyPrimaryTerm = 0,
        )
        val smPolicy = randomSMPolicy(
            seqNo = 1,
            primaryTerm = 1,
        )
        val updatedMetadata = randomSMMetadata(
            policySeqNo = 1,
            policyPrimaryTerm = 1,
        )

        val stateMachineSpy = spy(SMStateMachine(client, smPolicy, initialMetadata, settings, threadPool, indicesManager))

        val openSearchException = OpenSearchException("Test exception")
        doAnswer {
            val listener = it.getArgument<ActionListener<ActionResponse>>(1)
            listener.onFailure(openSearchException)
        }.whenever(client).index(any(), any())

        // Verify OpenSearchException is wrapped in SnapshotManagementException
        val thrownException = assertThrows(SnapshotManagementException::class.java) {
            runBlocking {
                stateMachineSpy.updateMetadata(updatedMetadata)
            }
        }

        // Verify exception type and cause
        assertTrue(thrownException.cause is OpenSearchException)
    }
}
