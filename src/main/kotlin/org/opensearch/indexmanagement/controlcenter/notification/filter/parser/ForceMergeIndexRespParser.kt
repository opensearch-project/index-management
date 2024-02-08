/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.opensearch.OpenSearchException
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
        callback: Consumer<ActionRespParseResult>,
    ) {
        if (ex != null) {
            callback.accept(
                ActionRespParseResult(
                    OperationResult.FAILED,
                    buildNotificationMessage(null, ex),
                    buildNotificationTitle(OperationResult.FAILED),
                ),
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
                    buildNotificationTitle(OperationResult.FAILED),
                ),
            )
        } else {
            callback.accept(
                ActionRespParseResult(
                    OperationResult.COMPLETE,
                    buildNotificationMessage(response),
                    buildNotificationTitle(OperationResult.COMPLETE),
                ),
            )
        }
    }

    override fun buildNotificationMessage(
        response: ForceMergeResponse?,
        exception: Exception?,
        isTimeout: Boolean,
    ): String {
        return if (exception != null) {
            if (exception is OpenSearchException) {
                "index [" + exception.index.name + "] ${exception.message}."
            } else {
                exception.message ?: ""
            }
        } else if (response != null && !response.shardFailures.isNullOrEmpty()) {
            response.shardFailures.joinToString(",") { "index [${it.index()}] shard [${it.shardId()}] ${it.reason()}" }
        } else if (request.indices().size == 1) {
            "The force merge operation on $indexNameWithCluster ${NotificationActionListener.COMPLETED}"
        } else {
            "$indexNameWithCluster have been merged."
        }
    }

    override fun buildNotificationTitle(operationResult: OperationResult): String {
        if (request.indices().size == 1) {
            return "Force merge operation on $indexNameWithCluster has ${getOperationResultTitleDesc(operationResult)}"
        } else {
            return "Force merge operation on " +
                "${request.indices().size} indexes from [${clusterService.clusterName.value()}] has ${getOperationResultTitleDesc(operationResult)}"
        }
    }
}
