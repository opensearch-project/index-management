/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.opensearch.OpenSearchException
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.admin.indices.open.OpenIndexResponse
import org.opensearch.action.support.ActiveShardCount
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.controlcenter.notification.filter.NotificationActionListener
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import java.lang.Exception
import java.util.function.Consumer

class OpenIndexRespParser(
    val activeShardsObserver: ActiveShardsObserver,
    val request: OpenIndexRequest,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    val clusterService: ClusterService,
) : ResponseParser<OpenIndexResponse> {

    private var totalWaitTime: TimeValue = NotificationActionListener.MAX_WAIT_TIME
    private val indexNameWithCluster = getIndexName(request, clusterService)

    @Suppress("LongMethod")
    override fun parseAndSendNotification(
        response: OpenIndexResponse?,
        ex: Exception?,
        callback: Consumer<ActionRespParseResult>,
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

        requireNotNull(response) { "OpenIndexResponse must not be null" }

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
                            ActionRespParseResult(
                                if (shardsAcknowledged) OperationResult.COMPLETE else OperationResult.TIMEOUT,
                                buildNotificationMessage(response, isTimeout = !shardsAcknowledged),
                                buildNotificationTitle(if (shardsAcknowledged) OperationResult.COMPLETE else OperationResult.TIMEOUT)
                            )
                        )
                    },
                    { e: Exception ->
                        // failed
                        callback.accept(
                            ActionRespParseResult(
                                OperationResult.FAILED,
                                buildNotificationMessage(response, e),
                                buildNotificationTitle(OperationResult.FAILED)
                            )
                        )
                    }
                )
            } else {
                callback.accept(
                    ActionRespParseResult(
                        OperationResult.TIMEOUT,
                        buildNotificationMessage(response, isTimeout = true),
                        buildNotificationTitle(OperationResult.TIMEOUT)
                    )
                )
            }
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
        response: OpenIndexResponse?,
        exception: Exception?,
        isTimeout: Boolean,
    ): String {
        val indexes = indexNameWithCluster + if (request.indices().size == 1) " has" else " have"

        return if (isTimeout)
            "Opening the index $indexes taken more than ${totalWaitTime.toHumanReadableString(1)} to complete. " +
                "To see the latest status, use `GET /${request.indices().joinToString(",")}/_recovery`"
        else if (exception != null)
            if (exception is OpenSearchException)
                "index [" + exception.index.name + "] ${exception.message}."
            else
                exception.message ?: ""
        else
            "$indexes been set to open."
    }

    override fun buildNotificationTitle(operationResult: OperationResult): String {
        val result =
            when (operationResult) {
                OperationResult.COMPLETE -> "been opened"
                OperationResult.FAILED -> "failed to open"
                else -> "timed out to open"
            }

        return if (request.indices().size == 1)
            "$indexNameWithCluster has $result"
        else
            "${request.indices().size} indexes from [${clusterService.clusterName.value()}] have $result"
    }
}
