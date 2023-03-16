/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filter.parser

import org.opensearch.common.collect.Tuple
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.indexmanagement.adminpanel.notification.filter.NotificationActionListener
import org.opensearch.indexmanagement.adminpanel.notification.filter.OperationResult
import org.opensearch.tasks.Task
import java.lang.Exception
import java.util.function.Consumer

class ReindexRespParser(val task: Task) : ResponseParser<BulkByScrollResponse> {

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
        val cancelled = response.reasonCancelled
        val result = StringBuilder()
        result.append("${task.description} ")
            .append(
                if (cancelled.isNullOrBlank() == false) {
                    "has been cancelled with reason: $cancelled"
                } else if (exception != null) {
                    NotificationActionListener.COMPLETED_WITH_ERROR + exception.message
                } else {
                    NotificationActionListener.COMPLETED
                }
            ).append(System.lineSeparator())
        // "took":8154,"timed_out":false,"total":14074,"updated":0,"created":14074,"deleted":0,
        result.append(
            "Details: total: ${response.total}, created: ${response.created}, " +
                "updated: ${response.updated}, deleted: ${response.deleted}"
        )

        if (!response.bulkFailures.isNullOrEmpty()) {
            result.append(
                "${System.lineSeparator()}Bulk Write Failures: ${
                response.bulkFailures.map { it.message }.toSet().joinToString(",")
                }"
            )
        }
        if (!response.searchFailures.isNullOrEmpty()) {
            result.append(
                "${System.lineSeparator()}Search Source Index Failures: ${
                response.searchFailures.map { it.reason.localizedMessage ?: "" }.toSet().joinToString(",")
                }"
            )
        }

        return result.toString()
    }
}
