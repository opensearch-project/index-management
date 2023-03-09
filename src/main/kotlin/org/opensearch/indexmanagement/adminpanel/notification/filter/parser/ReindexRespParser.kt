/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filter.parser

import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.indexmanagement.adminpanel.notification.filter.NotificationActionListener
import org.opensearch.tasks.Task
import java.lang.Exception

class ReindexRespParser(val task: Task) : ResponseParser<BulkByScrollResponse> {
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
            result.append("${System.lineSeparator()}Bulk Failures: ${response.bulkFailures.joinToString(",") { it.message }}")
        }
        if (!response.searchFailures.isNullOrEmpty()) {
            result.append(
                "${System.lineSeparator()}Search Failures: ${response.searchFailures.joinToString(",") { it.reason.localizedMessage ?: "" }}"
            )
        }

        return result.toString()
    }
}
