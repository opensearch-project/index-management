/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter

import org.junit.Assert
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.action.admin.indices.forcemerge.ForceMergeAction
import org.opensearch.action.admin.indices.open.OpenIndexAction
import org.opensearch.action.admin.indices.shrink.ResizeAction
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.client.Client
import org.opensearch.cluster.OpenSearchAllocationTestCase
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.tasks.TaskId
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.index.reindex.ReindexRequest
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool

class IndexOperationActionFilterTests : OpenSearchAllocationTestCase() {
    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var filter: IndexOperationActionFilter
    private lateinit var indexNameExpressionResolver: IndexNameExpressionResolver

    @Before
    @Throws(Exception::class)
    fun setup() {
        client = Mockito.mock(Client::class.java)
        val threadPool = Mockito.mock(ThreadPool::class.java)
        Mockito.`when`(client.threadPool()).thenReturn(threadPool)
        clusterService = Mockito.mock(ClusterService::class.java)
        Mockito.`when`(clusterService.localNode()).thenReturn(newNode("test-node-1"))

        val namedXContentRegistryEntries = arrayListOf<NamedXContentRegistry.Entry>()
        xContentRegistry = NamedXContentRegistry(namedXContentRegistryEntries)

        indexNameExpressionResolver = Mockito.mock(IndexNameExpressionResolver::class.java)

        val activeShardsObserver = ActiveShardsObserver(clusterService, client.threadPool())

        filter =
            IndexOperationActionFilter(
                this.client, clusterService, activeShardsObserver, indexNameExpressionResolver,
            )
    }

    fun `test wrapped listener for long running actions`() {
        val task = Mockito.mock(Task::class.java)
        Mockito.`when`(task.parentTaskId).thenReturn(TaskId.EMPTY_TASK_ID)
        val listener = TestActionListener<ActionResponse>()

        val wrappedActions = listOf(ReindexAction.NAME, ResizeAction.NAME, ForceMergeAction.NAME, OpenIndexAction.NAME)
        for (action in wrappedActions) {
            val newListener =
                filter.wrapActionListener(
                    task,
                    ReindexAction.NAME,
                    ReindexRequest(),
                    listener,
                )

            Assert.assertNotSame(listener, newListener)
            Assert.assertTrue(newListener is NotificationActionListener<*, *>)
        }
    }

    fun `test wrapped listener for other actions`() {
        val task = Mockito.mock(Task::class.java)
        Mockito.`when`(task.parentTaskId).thenReturn(TaskId.EMPTY_TASK_ID)
        val listener = TestActionListener<ActionResponse>()
        val newListener =
            filter.wrapActionListener(
                task,
                "test",
                ReindexRequest(),
                listener,
            )

        Assert.assertSame(listener, newListener)
    }

    fun `test wrapped listener for child task`() {
        val task = Mockito.mock(Task::class.java)
        Mockito.`when`(task.parentTaskId).thenReturn(TaskId("abc:1"))
        val listener = TestActionListener<ActionResponse>()
        val newListener =
            filter.wrapActionListener(
                task,
                ReindexAction.NAME,
                ReindexRequest(),
                listener,
            )

        Assert.assertSame(listener, newListener)
    }

    @Suppress("EmptyFunctionBlock")
    inner class TestActionListener<Response : ActionResponse> : ActionListener<Response> {
        override fun onResponse(response: Response) {}

        override fun onFailure(e: java.lang.Exception?) {}
    }
}
