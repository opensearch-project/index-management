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
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.controlcenter.notification.filter.NotificationActionListener
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import java.util.function.Consumer
import org.opensearch.action.admin.indices.shrink.ResizeResponse

class ResizeIndexRespParser(
    val activeShardsObserver: ActiveShardsObserver,
    val request: ResizeRequest,
    val clusterService: ClusterService
) : ResponseParser<ResizeResponse> {

    val logger: Logger = LogManager.getLogger(this::class.java)
    private var totalWaitTime: TimeValue = NotificationActionListener.MAX_WAIT_TIME
    private val indexWithCluster = getIndexName(request, clusterService)

    override fun parseAndSendNotification(
        response: ResizeResponse?,
        ex: Exception?,
        callback: Consumer<ActionRespParseResult>
    ) {
        val isAsync = request.shouldStoreResult
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
        requireNotNull(response) { "ResizeResponse must not be null" }

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
                        val result = if (shardsAcknowledged) OperationResult.COMPLETE else OperationResult.TIMEOUT
                        callback.accept(
                            ActionRespParseResult(
                                result,
                                buildNotificationMessage(response, isTimeout = !shardsAcknowledged),
                                buildNotificationTitle(result)
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
        response: ResizeResponse?,
        exception: Exception?,
        isTimeout: Boolean
    ): String {
        val result = StringBuilder()
        result.append(
            "The ${request.resizeType.name.lowercase()} job on from $indexWithCluster to ${
            getIndexName(
                request.targetIndexRequest, clusterService
            )
            } "
        ).append(
            if (isTimeout) {
                "has completed, but timed out while waiting for enough shards to be started in ${
                totalWaitTime.toHumanReadableString(1)
                }, try with `GET /${request.targetIndexRequest.index()}/_recovery` to get more index recovery details."
            } else if (exception != null) {
                "${NotificationActionListener.FAILED} ${exception.message}"
            } else {
                NotificationActionListener.COMPLETED
            }
        )
        return result.toString()
    }

    override fun buildNotificationTitle(operationResult: OperationResult): String {
        val builder = StringBuilder()
        with(builder) {
            append(request.resizeType.name.lowercase().replaceFirstChar { it.uppercase() })
            append(" on $indexWithCluster has ${operationResult.desc}.")
        }
        return builder.toString()
    }
}
