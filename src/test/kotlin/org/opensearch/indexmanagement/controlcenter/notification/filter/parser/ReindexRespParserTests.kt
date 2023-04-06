/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.junit.Assert
import org.junit.Before
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.opensearch.action.bulk.BulkItemResponse
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.BulkByScrollTask
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.tasks.Task
import org.opensearch.tasks.TaskId
import org.opensearch.test.OpenSearchTestCase
import java.lang.Exception
import java.util.concurrent.TimeUnit

class ReindexRespParserTests : OpenSearchTestCase() {

    private lateinit var task: Task
    private lateinit var clusterService: ClusterService

    @Before
    fun setup() {
        task = Task(1, "transport", ReindexAction.NAME, "reindex from src to dest", TaskId.EMPTY_TASK_ID, mapOf())
        clusterService = mock()
        val node = Mockito.mock<DiscoveryNode>()
        Mockito.`when`(node.id).thenReturn("mJzoy8SBuTW12rbV8jSg")
        Mockito.`when`(clusterService.localNode()).thenReturn(node)
    }

    fun `test build message for completion`() {
        val response = BulkByScrollResponse(
            TimeValue(1, TimeUnit.SECONDS),
            BulkByScrollTask.Status(
                1,
                100,
                0,
                100,
                0,
                1,
                0,
                0,
                0,
                0,
                TimeValue(0, TimeUnit.SECONDS),
                0.0f,
                "",
                TimeValue(0, TimeUnit.SECONDS)
            ),
            listOf(), listOf(), false
        )
        val parser = ReindexRespParser(task, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "Reindex from src to dest has completed." +
                System.lineSeparator() +
                "Details: total: 100, created: 100, updated: 0, deleted: 0"
        )
    }

    fun `test build message for cancellation`() {
        val response = BulkByScrollResponse(
            TimeValue(1, TimeUnit.SECONDS),
            BulkByScrollTask.Status(
                1,
                100,
                0,
                100,
                0,
                1,
                0,
                0,
                0,
                0,
                TimeValue(0, TimeUnit.SECONDS),
                0.0f,
                "user cancelled",
                TimeValue(0, TimeUnit.SECONDS)
            ),
            listOf(), listOf(), false
        )
        val parser = ReindexRespParser(task, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "Reindex from src to dest has been cancelled with reason: user cancelled" +
                System.lineSeparator() +
                "Details: total: 100, created: 100, updated: 0, deleted: 0"
        )
    }

    fun `test build message for failure`() {
        val response = BulkByScrollResponse(
            TimeValue(1, TimeUnit.SECONDS),
            BulkByScrollTask.Status(
                1,
                100,
                0,
                100,
                0,
                1,
                0,
                0,
                0,
                0,
                TimeValue(0, TimeUnit.SECONDS),
                0.0f,
                "",
                TimeValue(
                    0, TimeUnit.SECONDS
                )
            ),
            listOf(BulkItemResponse.Failure("dest", "id-1", Exception("version conflicts"))), listOf(), false
        )
        val parser = ReindexRespParser(task, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "Reindex from src to dest has completed." +
                System.lineSeparator() +
                "Details: total: 100, created: 100, updated: 0, deleted: 0" +
                "${System.lineSeparator()}There has some error happened, check with `GET /_tasks/mJzoy8SBuTW12rbV8jSg:1` to get detail."
        )
    }
}
