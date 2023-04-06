/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.Tuple
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.indexmanagement.controlcenter.notification.filter.NotificationActionListener
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import org.opensearch.tasks.Task
import org.opensearch.tasks.TaskId
import java.lang.Exception
import java.util.Locale
import java.util.function.Consumer

class ReindexRespParser(val task: Task, val clusterService: ClusterService) : ResponseParser<BulkByScrollResponse> {

    override fun parseAndSendNotification(
        response: BulkByScrollResponse,
        callback: Consumer<Tuple<OperationResult, String>>
    ) {
        val hasFailures = !response.bulkFailures.isNullOrEmpty() || !response.searchFailures.isNullOrEmpty()
        if (hasFailures) {
            callback.accept(Tuple.tuple(OperationResult.FAILED, buildNotificationMessage(response)))
        } else {
            callback.accept(Tuple.tuple(OperationResult.COMPLETE, buildNotificationMessage(response)))
        }
    }

    override fun buildNotificationMessage(
        response: BulkByScrollResponse,
        exception: Exception?,
        isTimeout: Boolean
    ): String {
        val reason = response.reasonCancelled
        val result = StringBuilder()
        with(result) {
            append("${task.description.replaceFirstChar { it.uppercase(Locale.getDefault()) }} ")
            append(
                if (reason.isNullOrBlank() == false) {
                    "has been cancelled with reason: $reason"
                } else if (exception != null) {
                    NotificationActionListener.COMPLETED_WITH_ERROR + exception.message
                } else {
                    NotificationActionListener.COMPLETED
                }
            )
            append(System.lineSeparator())
            // "took":8154,"timed_out":false,"total":14074,"updated":0,"created":14074,"deleted":0,
            append(
                "Details: total: ${response.total}, created: ${response.created}, updated: ${response.updated}, deleted: ${response.deleted}"
            )

            val taskId = TaskId(clusterService.localNode().id, task.id)

            if (!response.bulkFailures.isNullOrEmpty() || !response.searchFailures.isNullOrEmpty()) {
                append(System.lineSeparator())
                append("There has some error happened, check with `GET /_tasks/$taskId` to get detail.")
            }
        }

        return result.toString()
    }
}
