/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionResponse
import org.opensearch.action.index.IndexResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.ClusterAdminClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.indexmanagement.snapshotmanagement.mockIndexResponse
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool

abstract class MocksTestCase : OpenSearchTestCase() {

    val client: Client = mock()
    private val adminClient: AdminClient = mock()
    private val clusterAdminClient: ClusterAdminClient = mock()
    lateinit var settings: Settings
    lateinit var threadPool: ThreadPool
    lateinit var indicesManager: IndexManagementIndices

    @Before
    @Throws(Exception::class)
    fun setup() {
        threadPool = Mockito.mock(ThreadPool::class.java)
        settings = Settings.builder().build()
        Mockito.`when`(threadPool.threadContext).thenReturn(ThreadContext(settings))
        indicesManager = Mockito.mock(IndexManagementIndices::class.java)
    }

    @Before
    fun setupClient() {
        doAnswer {
            val listener = it.getArgument<ActionListener<IndexResponse>>(1)
            listener.onResponse(mockIndexResponse())
        }.whenever(client).index(any(), any())
    }

    fun mockCreateSnapshotCall(
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

    fun mockDeleteSnapshotCall(
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
        }.whenever(clusterAdminClient).deleteSnapshot(any(), any())
    }

    fun mockGetSnapshotsCall(
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
