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
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.action.index.IndexResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.ClusterAdminClient
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.getTestSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.getTestSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.mockCreateSnapshotResponse
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

        whenever(client.admin()).thenReturn(adminClient)
        whenever(adminClient.cluster()).thenReturn(clusterAdminClient)
        doAnswer {
            val listener = it.getArgument<ActionListener<CreateSnapshotResponse>>(1)
            listener.onResponse(mockCreateSnapshotResponse())
        }.whenever(clusterAdminClient).createSnapshot(any(), any())
    }

    fun `test create snapshot accepted`() = runBlocking {
        val metadata = getTestSMMetadata(
            currentState = SMState.CREATE_CONDITION_MET,
        )
        val job = getTestSMPolicy()
        val context = SMStateMachine(client, job, metadata)

        val end = SMState.CREATING.instance.execute(context)
        assertEquals("Execution should return true.", true, end)
        assertEquals("Current state should move to CREATING.", SMState.CREATING.toString(), context.metadataToSave!!.currentState)
    }
}
