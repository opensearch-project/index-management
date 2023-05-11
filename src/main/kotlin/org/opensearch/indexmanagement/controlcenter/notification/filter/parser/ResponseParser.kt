/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.IndicesRequest
import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.reindex.ReindexRequest
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import java.util.function.Consumer

interface ResponseParser<Response : ActionResponse> {
    fun parseAndSendNotification(
        response: Response?,
        ex: Exception? = null,
        callback: Consumer<ActionRespParseResult>
    ) {
        callback.accept(
            ActionRespParseResult(
                OperationResult.COMPLETE,
                buildNotificationMessage(response),
                buildNotificationTitle(OperationResult.COMPLETE)
            )
        )
    }

    fun buildNotificationMessage(
        response: Response?,
        exception: Exception? = null,
        isTimeout: Boolean = false
    ): String

    fun buildNotificationTitle(
        operationResult: OperationResult
    ): String

    fun getIndexName(req: ActionRequest, clusterService: ClusterService): String? {
        return when (req) {
            is IndicesRequest -> {
                "${clusterService.clusterName.value()}/${req.indices().joinToString(",")}"
            }

            is ReindexRequest -> {
                val clusterName =
                    if (req.remoteInfo != null) "remote cluster ${req.remoteInfo.host}" else clusterService.clusterName.value()
                "$clusterName/${req.searchRequest.indices().joinToString(",")}"
            }

            else -> {
                ""
            }
        }
    }
}
