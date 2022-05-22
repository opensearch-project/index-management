/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionResponse
import org.opensearch.action.index.IndexResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.ClusterAdminClient
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.Result
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.mockCreateSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockIndexResponse
import org.opensearch.test.OpenSearchTestCase

class CreatingStateTests : OpenSearchTestCase() {

    private val client: Client = mock()
    private val adminClient: AdminClient = mock()
    private val clusterAdminClient: ClusterAdminClient = mock()

    @Before
    fun setupClient() {
        doAnswer {
            val listener = it.getArgument<ActionListener<IndexResponse>>(1)
            listener.onResponse(mockIndexResponse())
        }.whenever(client).index(any(), any())
    }

    fun `test create snapshot succeed`() = runBlocking {
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))
        mockCreateSnapshotCall(response = mockCreateSnapshotResponse())

        val metadata = randomSMMetadata(
            currentState = SMState.CREATE_CONDITION_MET,
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val end = SMState.CREATING.instance.execute(context)
        assertTrue("Execution result should be Next.", end is Result.Next)
        end as Result.Next
        assertEquals("Current state should move to CREATING.", SMState.CREATING, end.metadataToSave.currentState)
    }

    fun `test create snapshot exception`() = runBlocking {
        val ex = Exception()
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))
        mockCreateSnapshotCall(exception = ex)

        val metadata = randomSMMetadata(
            currentState = SMState.CREATE_CONDITION_MET,
        )
        val job = randomSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val end = SMState.CREATING.instance.execute(context)
        assertTrue("Execution result should be Failure.", end is Result.Failure)
    }

    // fun `test undetermined atomic operation`() = runBlocking {
    //     val metadata = randomSMMetadata(
    //         currentState = SMState.CREATE_CONDITION_MET,
    //         atomic = true,
    //     )
    //     val job = randomSMPolicy()
    //     val context = SMStateMachine(client, job, metadata)
    //
    //     val end = SMState.CREATING.instance.execute(context)
    //     assertTrue("Execution result should be failure.", end is ExecutionResult.Failure)
    //     end as ExecutionResult.Failure
    //     val ex = end.ex
    //     assertTrue("Failure exception should be StateMachineException.", ex is SnapshotManagementException)
    //     ex as SnapshotManagementException
    //     assertTrue("StateMachineException error code should be ATOMIC", ex.exKey == ATOMIC)
    // }

    private fun mockCreateSnapshotCall(
        response: ActionResponse? = null,
        exception: Exception? = null
    ) {
        assertTrue(
            "Must provide either a response or an exception.",
            (response != null).xor(exception != null)
        )
        whenever(client.admin()).thenReturn(adminClient)
        whenever(adminClient.cluster()).thenReturn(clusterAdminClient)
        doAnswer {
            val listener = it.getArgument<ActionListener<ActionResponse>>(1)
            if (response != null) listener.onResponse(response)
            else listener.onFailure(exception)
        }.whenever(clusterAdminClient).createSnapshot(any(), any())
    }

    private fun mockGetSnapshotsCall(
        response: ActionResponse? = null,
        exception: Exception? = null
    ) {
        assertTrue(
            "Must provide either a response or an exception.",
            (response != null).xor(exception != null)
        )
        whenever(client.admin()).thenReturn(adminClient)
        whenever(adminClient.cluster()).thenReturn(clusterAdminClient)
        doAnswer {
            val listener = it.getArgument<ActionListener<ActionResponse>>(1)
            if (response != null) listener.onResponse(response)
            else listener.onFailure(exception)
        }.whenever(clusterAdminClient).getSnapshots(any(), any())
    }
}
