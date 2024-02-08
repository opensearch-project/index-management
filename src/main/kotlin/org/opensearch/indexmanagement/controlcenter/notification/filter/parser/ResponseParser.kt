/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.opensearch.action.ActionRequest
import org.opensearch.action.IndicesRequest
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.action.ActionResponse
import org.opensearch.index.reindex.ReindexRequest
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import java.util.function.Consumer

interface ResponseParser<Response : ActionResponse> {
    fun parseAndSendNotification(
        response: Response?,
        ex: Exception? = null,
        callback: Consumer<ActionRespParseResult>,
    )

    fun buildNotificationMessage(
        response: Response?,
        exception: Exception? = null,
        isTimeout: Boolean = false,
    ): String

    fun buildNotificationTitle(
        operationResult: OperationResult,
    ): String

    fun getIndexName(req: ActionRequest, clusterService: ClusterService): String? {
        var clusterName = clusterService.clusterName.value()
        return when (req) {
            is IndicesRequest -> {
                if (req.indices().size == 1) {
                    "[$clusterName/${req.indices().joinToString(",")}]"
                } else {
                    "[${req.indices().joinToString(",")}] from [$clusterName]"
                }
            }

            is ReindexRequest -> {
                clusterName =
                    if (req.remoteInfo != null) "remote cluster ${req.remoteInfo.host}" else clusterName
                if (req.searchRequest.indices().size == 1) {
                    "[$clusterName/${req.searchRequest.indices().joinToString(",")}]"
                } else {
                    "[${req.searchRequest.indices().joinToString(",")}] from [$clusterName]"
                }
            }

            else -> {
                ""
            }
        }
    }

    fun getOperationResultTitleDesc(result: OperationResult): String {
        return when (result) {
            OperationResult.COMPLETE -> "completed"
            OperationResult.FAILED -> "failed"
            OperationResult.TIMEOUT -> "timed out"
            OperationResult.CANCELLED -> "been cancelled"
        }
    }
}
