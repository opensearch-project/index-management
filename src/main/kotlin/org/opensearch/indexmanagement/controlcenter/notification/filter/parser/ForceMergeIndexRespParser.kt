/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.controlcenter.notification.filter.NotificationActionListener
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import java.lang.Exception
import java.util.function.Consumer

class ForceMergeIndexRespParser(val request: ForceMergeRequest, val clusterService: ClusterService) :
    ResponseParser<ForceMergeResponse> {

    private val indexNameWithCluster = getIndexName(request, clusterService)

    override fun parseAndSendNotification(
        response: ForceMergeResponse?,
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

        requireNotNull(response) { "ForceMergeResponse must not be null" }

        val hasFailures = response.shardFailures != null && response.shardFailures.isNotEmpty()
        if (hasFailures) {
            callback.accept(
                ActionRespParseResult(
                    OperationResult.FAILED,
                    buildNotificationMessage(response),
                    buildNotificationTitle(OperationResult.FAILED)
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
        response: ForceMergeResponse?,
        exception: Exception?,
        isTimeout: Boolean
    ): String {
        val result = StringBuilder()
        result.append(
            "The force merge job on $indexNameWithCluster " +
                if (exception != null) {
                    "${NotificationActionListener.FAILED} ${exception.message}"
                } else if (response != null && !response.shardFailures.isNullOrEmpty()) {
                    "${NotificationActionListener.FAILED} ${response.shardFailures.joinToString(",") { it.reason() }}"
                } else {
                    NotificationActionListener.COMPLETED
                }
        )
        return result.toString()
    }

    override fun buildNotificationTitle(operationResult: OperationResult): String {
        return "Force merge on $indexNameWithCluster has ${operationResult.desc}."
    }
}
