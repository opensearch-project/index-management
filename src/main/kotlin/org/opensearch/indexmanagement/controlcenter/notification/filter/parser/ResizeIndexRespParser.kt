/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.admin.indices.shrink.ResizeResponse
import org.opensearch.action.support.ActiveShardCount
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.controlcenter.notification.filter.NotificationActionListener
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import java.lang.IllegalStateException
import java.util.function.Consumer

class ResizeIndexRespParser(
    val activeShardsObserver: ActiveShardsObserver,
    val request: ResizeRequest,
    val clusterService: ClusterService,
) : ResponseParser<ResizeResponse> {
    val logger: Logger = LogManager.getLogger(this::class.java)
    private var totalWaitTime: TimeValue = NotificationActionListener.MAX_WAIT_TIME
    private val indexWithCluster = getIndexName(request, clusterService)

    @Suppress("LongMethod")
    override fun parseAndSendNotification(
        response: ResizeResponse?,
        ex: Exception?,
        callback: Consumer<ActionRespParseResult>,
    ) {
        val isAsync = request.shouldStoreResult
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
                                buildNotificationTitle(result),
                            ),
                        )
                    },
                    { e: Exception ->
                        // failed
                        callback.accept(
                            ActionRespParseResult(
                                OperationResult.FAILED,
                                buildNotificationMessage(response, e),
                                buildNotificationTitle(OperationResult.FAILED),
                            ),
                        )
                    },
                )
            } else {
                callback.accept(
                    ActionRespParseResult(
                        OperationResult.TIMEOUT,
                        buildNotificationMessage(response, isTimeout = true),
                        buildNotificationTitle(OperationResult.TIMEOUT),
                    ),
                )
            }
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
        response: ResizeResponse?,
        exception: Exception?,
        isTimeout: Boolean,
    ): String {
        val result = StringBuilder()
        val action = request.resizeType.name.lowercase()
        result.append(
            "The $action operation from $indexWithCluster to ${
                getIndexName(request.targetIndexRequest, clusterService)
            } ",
        ).append(
            if (isTimeout) {
                "has taken more than ${totalWaitTime.toHumanReadableString(1)} to complete. " +
                    "To see the latest status, use `GET /${request.targetIndexRequest.index()}/_recovery`"
            } else if (exception != null) {
                val message = exception.message ?: ""
                return when (exception) {
                    is IllegalStateException -> {
                        when {
                            message.contains("must have all shards allocated on the same node") -> {
                                "You must allocate a copy of every shard of the source index to the same node before $action. " +
                                    "To allocate it to same node, try use PUT /${request.sourceIndex}/_settings\n" +
                                    "{\n" +
                                    "\"index.routing.allocation.require._name\":\"your_node_name\"\n" +
                                    "}"
                            }

                            message.contains("must block write operations to resize index") -> {
                                "$indexWithCluster must be set to block write to $action the index. " +
                                    "To set it to block write, use `PUT /${request.sourceIndex}/_block/write` "
                            }

                            else -> message
                        }
                    }

                    is ResourceAlreadyExistsException -> {
                        "The target index ${getIndexName(request.targetIndexRequest, clusterService)} already exists."
                    }

                    is IndexNotFoundException -> "The $indexWithCluster does not exist."

                    else -> message
                }
            } else {
                NotificationActionListener.COMPLETED
            },
        )
        return result.toString()
    }

    override fun buildNotificationTitle(operationResult: OperationResult): String {
        val builder = StringBuilder()
        with(builder) {
            append(request.resizeType.name.lowercase().replaceFirstChar { it.uppercase() })
            append(" operation on $indexWithCluster has ${getOperationResultTitleDesc(operationResult)}")
        }
        return builder.toString()
    }
}
