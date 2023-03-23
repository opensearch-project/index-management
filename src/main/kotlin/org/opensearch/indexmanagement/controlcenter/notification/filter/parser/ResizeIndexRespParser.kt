/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.support.ActiveShardCount
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.common.collect.Tuple
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.controlcenter.notification.filter.NotificationActionListener
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import java.lang.Exception
import java.util.function.Consumer
import org.opensearch.action.admin.indices.shrink.ResizeResponse as ResizeResponse

class ResizeIndexRespParser(
    val activeShardsObserver: ActiveShardsObserver,
    val request: ResizeRequest
) : ResponseParser<ResizeResponse> {

    val logger: Logger = LogManager.getLogger(this::class.java)
    private var totalWaitTime: TimeValue = NotificationActionListener.MAX_WAIT_TIME

    override fun parseAndSendNotification(
        response: ResizeResponse,
        callback: Consumer<Tuple<OperationResult, String>>
    ) {
        val isAsync = request.shouldStoreResult
        if (response.isShardsAcknowledged == false) {
            logger.debug("Not all shards are started, continue monitoring on shards status")
            // the elapsedTime is the time user already waiting for, which is set through request parameter. the default value is 30s
            // the maximum wait time is 1 hour and the left is maximum - elapsed
            val elapsedTime = request.targetIndexRequest.timeout()
            totalWaitTime = elapsedTime
            val leftTimeInMillis = NotificationActionListener.MAX_WAIT_TIME.millis - elapsedTime.millis
            if (isAsync == false && leftTimeInMillis > 0) {

                totalWaitTime = NotificationActionListener.MAX_WAIT_TIME

                activeShardsObserver.waitForActiveShards(
                    arrayOf<String>(response.index()),
                    ActiveShardCount.DEFAULT, // once all primary shards are started, we think it is completed
                    TimeValue(leftTimeInMillis),
                    { shardsAcknowledged: Boolean ->
                        callback.accept(
                            Tuple.tuple(
                                if (shardsAcknowledged) OperationResult.COMPLETE else OperationResult.TIMEOUT,
                                buildNotificationMessage(response, isTimeout = !shardsAcknowledged)
                            )
                        )
                    },
                    { e: Exception ->
                        // failed
                        callback.accept(Tuple.tuple(OperationResult.FAILED, buildNotificationMessage(response, e)))
                    }
                )
            } else {
                callback.accept(Tuple.tuple(OperationResult.TIMEOUT, buildNotificationMessage(response, isTimeout = true)))
            }
        } else {
            callback.accept(Tuple.tuple(OperationResult.COMPLETE, buildNotificationMessage(response)))
        }
    }

    override fun buildNotificationMessage(response: ResizeResponse, exception: Exception?, isTimeout: Boolean): String {
        val result = StringBuilder()
        result.append("${request.resizeType.name.lowercase()} from ${request.sourceIndex} to ${request.targetIndexRequest.index()} ")
            .append(
                if (isTimeout) {
                    "has completed, but timed out while waiting for enough shards to be started in ${
                    totalWaitTime.toHumanReadableString(1)
                    }, try with `GET /<target>/_recovery` to get more details."
                } else if (exception != null) {
                    "${NotificationActionListener.COMPLETED_WITH_ERROR} ${exception.message}"
                } else {
                    NotificationActionListener.COMPLETED
                }
            )
        return result.toString()
    }
}
