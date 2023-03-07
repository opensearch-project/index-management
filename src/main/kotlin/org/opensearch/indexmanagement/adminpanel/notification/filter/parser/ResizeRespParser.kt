/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filter.parser

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.support.ActiveShardCount
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.indexmanagement.adminpanel.notification.filter.NotificationActionListener
import java.lang.Exception
import java.util.function.Consumer
import org.opensearch.action.admin.indices.shrink.ResizeResponse as ResizeResponse

class ResizeRespParser(
    val activeShardsObserver: ActiveShardsObserver,
    val request: ResizeRequest
) : ResponseParser<ResizeResponse> {

    val logger: Logger = LogManager.getLogger(this::class.java)

    override fun parseAndSendNotification(
        response: ResizeResponse,
        callback: Consumer<String>
    ) {
        if (response.isShardsAcknowledged == false) {
            logger.debug("Not all shards are started, continue monitoring on shards status")
            activeShardsObserver.waitForActiveShards(
                arrayOf<String>(response.index()),
                ActiveShardCount.DEFAULT, // once all primary shards are started, we think it is completed
                NotificationActionListener.MAX_WAIT_TIME,
                { shardsAcknowledged: Boolean ->
                    if (shardsAcknowledged == false) {
                        callback.accept(buildNotificationMessage(response))
                    } else {
                        callback.accept(buildNotificationMessage(response, isTimeout = true))
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

    override fun buildNotificationMessage(response: ResizeResponse, exception: Exception?, isTimeout: Boolean): String {
        val result = StringBuilder()
        result.append("${request.resizeType.name.lowercase()} from ${request.sourceIndex} to ${request.targetIndexRequest.index()} ")
            .append(
                if (isTimeout == true) {
                    NotificationActionListener.COMPLETED_WITH_TIMEOUT
                } else if (exception != null) {
                    "${NotificationActionListener.COMPLETED_WITH_ERROR} ${exception.message}"
                } else {
                    NotificationActionListener.COMPLETED
                }
            )
        return result.toString()
    }
}
