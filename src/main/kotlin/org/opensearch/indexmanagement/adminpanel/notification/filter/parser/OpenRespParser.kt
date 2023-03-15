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
import org.opensearch.common.collect.Tuple
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.adminpanel.notification.filter.NotificationActionListener
import org.opensearch.indexmanagement.adminpanel.notification.filter.OperationResult
import java.lang.Exception
import java.util.function.Consumer

class OpenRespParser(
    val activeShardsObserver: ActiveShardsObserver,
    val request: OpenIndexRequest,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    var clusterService: ClusterService
) : ResponseParser<OpenIndexResponse> {

    private var totalWaitTime: TimeValue = NotificationActionListener.MAX_WAIT_TIME

    override fun parseAndSendNotification(
        response: OpenIndexResponse,
        callback: Consumer<Tuple<OperationResult, String>>
    ) {
        val isAsync = request.shouldStoreResult
        // the elapsedTime is the time user already waiting for, which is set through request parameter. the default value is 30s
        // the maximum wait time is 1 hour and the left is maximum - elapsed
        val elapsedTime = request.ackTimeout()
        totalWaitTime = elapsedTime
        val leftTimeInMillis = NotificationActionListener.MAX_WAIT_TIME.millis - elapsedTime.millis
        if (response.isShardsAcknowledged == false) {
            if (isAsync == false && leftTimeInMillis > 0) {
                totalWaitTime = NotificationActionListener.MAX_WAIT_TIME
                val concreteIndices =
                    indexNameExpressionResolver.concreteIndexNames(clusterService.state(), request)

                activeShardsObserver.waitForActiveShards(
                    concreteIndices,
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
                        callback.accept(
                            Tuple.tuple(
                                OperationResult.FAILED,
                                buildNotificationMessage(response, e)
                            )
                        )
                    }
                )
            } else {
                callback.accept(
                    Tuple.tuple(
                        OperationResult.TIMEOUT,
                        buildNotificationMessage(response, isTimeout = true)
                    )
                )
            }
        } else {
            callback.accept(Tuple.tuple(OperationResult.COMPLETE, buildNotificationMessage(response)))
        }
    }

    override fun buildNotificationMessage(
        response: OpenIndexResponse,
        exception: Exception?,
        isTimeout: Boolean
    ): String {
        val result = StringBuilder()
        result.append(
            "open index [${request.indices().joinToString(",")}] " +
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
