/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.junit.Assert
import org.junit.Before
import org.opensearch.OpenSearchException
import org.opensearch.action.bulk.BulkItemResponse
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.index.Index
import org.opensearch.core.tasks.TaskId
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.BulkByScrollTask
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.index.reindex.ReindexRequest
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import org.opensearch.tasks.Task
import java.lang.Exception
import java.util.concurrent.TimeUnit

class ReindexRespParserTests : BaseRespParserTests() {

    private lateinit var task: Task
    private lateinit var request: ReindexRequest

    @Before
    fun setup() {
        task = Task(1, "transport", ReindexAction.NAME, "reindex from src to dest", TaskId.EMPTY_TASK_ID, mapOf())
        request = ReindexRequest().also {
            it.searchRequest.indices("source")
            it.destination.index("dest")
        }
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
                TimeValue(0, TimeUnit.SECONDS),
            ),
            listOf(), listOf(), false,
        )
        val parser = ReindexRespParser(task, request, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "The reindex operation from [test-cluster/source] to [test-cluster/dest] has been completed.\n" +
                "\n" +
                "*Summary (number of documents)* \n" +
                "Total: 100, Created: 100, Updated: 0, Deleted: 0, Conflicts: 0",
        )

        val title = parser.buildNotificationTitle(OperationResult.COMPLETE)
        Assert.assertEquals(title, "Reindex operation on [test-cluster/source] has completed")
    }

    fun `test build message for cancellation`() {
        val response = BulkByScrollResponse(
            TimeValue(1, TimeUnit.SECONDS),
            BulkByScrollTask.Status(
                1,
                100,
                0,
                20,
                0,
                1,
                0,
                0,
                0,
                0,
                TimeValue(0, TimeUnit.SECONDS),
                0.0f,
                "user cancelled",
                TimeValue(0, TimeUnit.SECONDS),
            ),
            listOf(), listOf(), false,
        )
        val parser = ReindexRespParser(task, request, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "The reindex operation from [test-cluster/source] to [test-cluster/dest] has been cancelled by user's request\n" +
                "\n" +
                "*Summary (number of documents)* \n" +
                "Total: 100, Created: 20, Updated: 0, Deleted: 0, Conflicts: 0",
        )

        val title = parser.buildNotificationTitle(OperationResult.CANCELLED)
        Assert.assertEquals(title, "Reindex operation on [test-cluster/source] has been cancelled")
    }

    fun `test build message for failure`() {
        val response = BulkByScrollResponse(
            TimeValue(1, TimeUnit.SECONDS),
            BulkByScrollTask.Status(
                1,
                100,
                0,
                99,
                0,
                1,
                1,
                0,
                0,
                0,
                TimeValue(0, TimeUnit.SECONDS),
                0.0f,
                "",
                TimeValue(
                    0, TimeUnit.SECONDS,
                ),
            ),
            listOf(BulkItemResponse.Failure("dest", "id-1", Exception("version conflicts"))), listOf(), false,
        )
        val parser = ReindexRespParser(task, request, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "The reindex operation from [test-cluster/source] to [test-cluster/dest] has failed. \n" +
                "\n" +
                " 1 error(s) found, including: \n" +
                "version conflicts\n" +
                "To see full errors, use `GET /_tasks/mJzoy8SBuTW12rbV8jSg:1`\n" +
                "\n" +
                "*Summary (number of documents)* \n" +
                "Total: 100, Created: 99, Updated: 0, Deleted: 0, Conflicts: 1",
        )

        val title = parser.buildNotificationTitle(OperationResult.FAILED)
        Assert.assertEquals(title, "Reindex operation on [test-cluster/source] has failed")
    }

    fun `test build message for exception`() {
        val parser = ReindexRespParser(task, request, clusterService)
        val ex = OpenSearchException("index doest not exists")
        ex.index = Index("source", "uuid")

        parser.parseAndSendNotification(null, ex) { ret ->
            Assert.assertEquals(ret.operationResult, OperationResult.FAILED)
            Assert.assertEquals(ret.title, "Reindex operation on [test-cluster/source] has failed")
            Assert.assertEquals(
                ret.message,
                "The reindex operation from [test-cluster/source] to [test-cluster/dest] has failed. index doest not exists",
            )
        }
    }
}
