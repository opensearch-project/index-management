/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filter.parser

import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.admin.indices.open.OpenIndexResponse
import org.opensearch.action.support.ActiveShardCount
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.adminpanel.notification.filter.NotificationActionListener
import java.lang.Exception
import java.util.function.Consumer

class OpenRespParser(
    val activeShardsObserver: ActiveShardsObserver,
    val request: OpenIndexRequest,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    var clusterService: ClusterService
) : ResponseParser<OpenIndexResponse> {
    override fun parseAndSendNotification(
        response: OpenIndexResponse,
        callback: Consumer<String>
    ) {
        if (response.isShardsAcknowledged == false) {
            val concreteIndices =
                indexNameExpressionResolver.concreteIndices(clusterService.state(), request).map { it.name }
            activeShardsObserver.waitForActiveShards(
                concreteIndices.toTypedArray(),
                ActiveShardCount.DEFAULT, // once all primary shards are started, we think it is completed
                NotificationActionListener.MAX_WAIT_TIME,
                { shardsAcknowledged: Boolean ->
                    if (shardsAcknowledged == false) {
                        callback.accept(buildNotificationMessage(response, isTimeout = true))
                    } else {
                        callback.accept(buildNotificationMessage(response))
                    }
                },
                { e: Exception ->
                    // failed
                    callback.accept(buildNotificationMessage(response, e))
                }
            )
        } else {
            callback.accept(buildNotificationMessage(response))
        }
    }

    override fun buildNotificationMessage(
        response: OpenIndexResponse,
        exception: Exception?,
        isTimeout: Boolean
    ): String {
        val result = StringBuilder()
        result.append(
            "open index [${request.indices().joinToString(",")}] ${NotificationActionListener.COMPLETED}"
        )

        return result.toString()
    }
}
