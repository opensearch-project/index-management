/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.ReindexRequest
import org.opensearch.indexmanagement.controlcenter.notification.filter.NotificationActionListener
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import org.opensearch.tasks.Task
import org.opensearch.tasks.TaskId
import java.util.function.Consumer

class ReindexRespParser(
    val task: Task,
    val request: ReindexRequest,
    val clusterService: ClusterService
) : ResponseParser<BulkByScrollResponse> {

    private val sourceIndex = getIndexName(request, clusterService)

    override fun parseAndSendNotification(
        response: BulkByScrollResponse?,
        ex: Exception?,
        callback: Consumer<ActionRespParseResult>
    ) {
        if (ex != null) {
            callback.accept(
                ActionRespParseResult(
                    OperationResult.FAILED,
                    buildNotificationMessage(null, ex),
                    buildNotificationTitle(OperationResult.FAILED)
                )
            )
            return
        }
        requireNotNull(response) { "Reindex response must not be null" }

        val hasFailures = !response.bulkFailures.isNullOrEmpty() || !response.searchFailures.isNullOrEmpty()
        if (hasFailures) {
            callback.accept(
                ActionRespParseResult(
                    OperationResult.FAILED,
                    buildNotificationMessage(response),
                    buildNotificationTitle(OperationResult.FAILED)
                )
            )
        } else if (!response.reasonCancelled.isNullOrEmpty()) {
            callback.accept(
                ActionRespParseResult(
                    OperationResult.FAILED,
                    buildNotificationMessage(response),
                    buildNotificationTitle(OperationResult.CANCELLED)
                )
            )
        } else {
            callback.accept(
                ActionRespParseResult(
                    OperationResult.COMPLETE,
                    buildNotificationMessage(response),
                    buildNotificationTitle(OperationResult.COMPLETE)
                )
            )
        }
    }

    override fun buildNotificationMessage(
        response: BulkByScrollResponse?,
        exception: Exception?,
        isTimeout: Boolean
    ): String {
        val result = StringBuilder(
            "The reindex operation from $sourceIndex to ${getIndexName(request.destination, clusterService)} "
        )
        if (exception != null) {
            result.append("${NotificationActionListener.FAILED} ${exception.message}")
            return result.toString()
        }

        requireNotNull(response) { "Reindex response must not be null" }

        val reason = response.reasonCancelled
        val failures = mutableListOf<Throwable>()
        if (!response.bulkFailures.isNullOrEmpty()) {
            failures.addAll(response.bulkFailures.map { it.cause })
        }
        if (!response.searchFailures.isNullOrEmpty()) {
            failures.addAll(response.searchFailures.map { it.reason })
        }
        val failed = failures.isNotEmpty()

        val taskId = TaskId(clusterService.localNode().id, task.id)

        with(result) {
            append(
                if (!reason.isNullOrBlank()) {
                    "has been cancelled by user's request"
                } else if (failed) {
                    "${NotificationActionListener.FAILED} \n\n ${failures.size} error(s) found, including: \n" +
                        failures.map { it.message }.distinct().take(2).joinToString(",") +
                        "\nTo see full errors, use `GET /_tasks/$taskId`"
                } else {
                    NotificationActionListener.COMPLETED
                }
            )
            append("\n\n")
            append("*Summary (number of documents)* \n")
            append(
                "Total: ${response.total}, Created: ${response.created}, " +
                    "Updated: ${response.updated}, Deleted: ${response.deleted}, " +
                    "Conflicts: ${response.versionConflicts}"
            )
        }

        return result.toString()
    }

    override fun buildNotificationTitle(operationResult: OperationResult): String {
        return "Reindex operation on $sourceIndex has ${getOperationResultTitleDesc(operationResult)}"
    }
}
