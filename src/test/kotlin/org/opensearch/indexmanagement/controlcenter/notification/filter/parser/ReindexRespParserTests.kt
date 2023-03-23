/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.junit.Assert
import org.junit.Before
import org.opensearch.action.bulk.BulkItemResponse
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

    @Before
    fun setup() {
        task = Task(1, "transport", ReindexAction.NAME, "reindex from src to dest", TaskId.EMPTY_TASK_ID, mapOf())
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
        val parser = ReindexRespParser(task)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "reindex from src to dest has completed." +
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
        val parser = ReindexRespParser(task)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "reindex from src to dest has been cancelled with reason: user cancelled" +
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
        val parser = ReindexRespParser(task)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(
            msg,
            "reindex from src to dest has completed." +
                System.lineSeparator() +
                "Details: total: 100, created: 100, updated: 0, deleted: 0" +
                "${System.lineSeparator()}Bulk Write Failures: java.lang.Exception: version conflicts"
        )
    }
}
