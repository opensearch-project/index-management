/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.admin.indices.open.OpenIndexResponse
import org.opensearch.action.admin.indices.shrink.ResizeAction
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.admin.indices.shrink.ResizeResponse
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.Tuple
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.indexmanagement.controlcenter.notification.LRONConfigResponse
import org.opensearch.indexmanagement.controlcenter.notification.action.delete.DeleteLRONConfigAction
import org.opensearch.indexmanagement.controlcenter.notification.action.delete.DeleteLRONConfigRequest
import org.opensearch.indexmanagement.controlcenter.notification.filter.parser.ForceMergeIndexRespParser
import org.opensearch.indexmanagement.controlcenter.notification.filter.parser.OpenIndexRespParser
import org.opensearch.indexmanagement.controlcenter.notification.filter.parser.ReindexRespParser
import org.opensearch.indexmanagement.controlcenter.notification.filter.parser.ResizeIndexRespParser
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.util.DEFAULT_LRON_CONFIG_SORT_FIELD
import org.opensearch.indexmanagement.controlcenter.notification.util.getDocID
import org.opensearch.indexmanagement.common.model.rest.DEFAULT_PAGINATION_SIZE
import org.opensearch.indexmanagement.common.model.rest.SORT_ORDER_DESC
import org.opensearch.indexmanagement.common.model.rest.SearchParams
import org.opensearch.indexmanagement.controlcenter.notification.action.get.GetLRONConfigAction
import org.opensearch.indexmanagement.controlcenter.notification.action.get.GetLRONConfigRequest
import org.opensearch.indexmanagement.controlcenter.notification.action.get.GetLRONConfigResponse
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.tasks.Task
import org.opensearch.tasks.TaskId
import org.opensearch.threadpool.ThreadPool.Names.GENERIC
import java.util.function.Consumer

@Suppress("LongParameterList", "MaxLineLength")
@OpenForTesting
class NotificationActionListener<Request : ActionRequest, Response : ActionResponse>(
    val delegate: ActionListener<Response>,
    val client: Client,
    val clusterService: ClusterService,
    val action: String,
    val task: Task,
    val activeShardsObserver: ActiveShardsObserver,
    val request: Request,
    val indexNameExpressionResolver: IndexNameExpressionResolver
) : ActionListener<Response>,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("NotificationActionListener")) {

    private val logger = LogManager.getLogger(NotificationActionListener::class.java)

    @Suppress("MagicNumber")
    private val notificationRetryPolicy = BackoffPolicy.exponentialBackoff(TimeValue(1000), 3)

    override fun onResponse(response: Response) {
        try {
            delegate.onResponse(response)
            parseAndSendNotification(response)
        } catch (e: Exception) {
            delegate.onFailure(e)
        }
    }

    override fun onFailure(e: Exception) {
        try {
            delegate.onFailure(e)
        } finally {
            var actionName = action
            if (action == ResizeAction.NAME) {
                actionName = (request as ResizeRequest).resizeType.name.lowercase()
            }
            notify(action, OperationResult.FAILED, "$actionName execute failed with error: ${e.message}")
        }
    }

    fun parseAndSendNotification(response: ActionResponse) {
        try {
            val callback =
                Consumer<Tuple<OperationResult, String>> { result -> notify(action, result.v1(), result.v2()) }

            when (response) {
                is ResizeResponse -> ResizeIndexRespParser(
                    activeShardsObserver, request as ResizeRequest
                ).parseAndSendNotification(response, callback)

                is BulkByScrollResponse -> ReindexRespParser(task).parseAndSendNotification(response, callback)

                is OpenIndexResponse -> OpenIndexRespParser(
                    activeShardsObserver, request as OpenIndexRequest, indexNameExpressionResolver, clusterService
                ).parseAndSendNotification(response, callback)

                is ForceMergeResponse -> ForceMergeIndexRespParser(request as ForceMergeRequest).parseAndSendNotification(
                    response, callback
                )

                else -> {
                    logger.debug("Action: {} is not supported for notification!", action)
                }
            }
        } catch (e: Exception) {
            logger.info("Sending out notification for action: {} failed", action, e)
        }
    }

    fun notify(
        action: String,
        result: OperationResult,
        defaultMessage: String
    ) {
        val taskId = TaskId(clusterService.localNode().id, task.id)
        val ids = arrayOf(getDocID(taskId = taskId), getDocID(actionName = action))
        val queryString = "_id:(${ids.joinToString(" OR ") { escapeQueryString(it) }})"
        val searchParam = SearchParams(
            DEFAULT_PAGINATION_SIZE, 0, DEFAULT_LRON_CONFIG_SORT_FIELD, SORT_ORDER_DESC, queryString
        )

        client.threadPool().threadContext.stashContext().use {
            client.execute(
                GetLRONConfigAction.INSTANCE,
                GetLRONConfigRequest(searchParams = searchParam),
                object : ActionListener<GetLRONConfigResponse> {
                    override fun onResponse(lronConfigResponse: GetLRONConfigResponse) {
                        sendNotification(lronConfigResponse, taskId, action, result, defaultMessage)
                    }

                    override fun onFailure(e: Exception) {
                        if (e is IndexNotFoundException) {
                            logger.debug("No notification policy configured for task: {} action: {}", taskId.toString(), action)
                        } else {
                            logger.error("Can't get notification policy for action: {}", action, e)
                        }
                    }
                }
            )
        }
    }

    private fun sendNotification(
        lronConfigResponse: GetLRONConfigResponse,
        taskId: TaskId,
        action: String,
        result: OperationResult,
        defaultMessage: String
    ) {
        if (0 == lronConfigResponse.totalNumber) {
            logger.debug("No notification policy configured for task: {} action: {}", taskId.toString(), action)
            return
        }

        val channels = getQualifiedChannels(lronConfigResponse, result)
        val eventSource = EventSource(TITLE, taskId.toString(), SeverityType.INFO)

        channels.forEach {
            // delay the sending time 5s for runtime policy
            client.threadPool().schedule({
                launch {
                    val config = it.lronConfig
                    if (config.lronCondition.isEnabled()) {
                        it.lronConfig.channels?.forEach { channel ->
                            try {
                                notificationRetryPolicy.retry(logger) {
                                    channel.sendNotification(
                                        client,
                                        eventSource,
                                        defaultMessage,
                                        config.user,
                                    )
                                }
                            } catch (osse: OpenSearchStatusException) {
                                logger.warn(
                                    "Sending notification failed, restStatus {}", osse.status(), osse
                                )
                            } catch (e: Exception) {
                                logger.error("Sending notification failed", e)
                            }
                        }
                    }

                    // remove one time configuration no matter it is enabled or not
                    removeOneTimePolicy(config)
                }
            }, DELAY, GENERIC)
        }
    }

    fun removeOneTimePolicy(
        config: LRONConfig
    ) {
        if (config.taskId != null) {
            val taskId = config.taskId
            client.execute(
                DeleteLRONConfigAction.INSTANCE,
                DeleteLRONConfigRequest(
                    getDocID(taskId = taskId)
                ),
                object : ActionListener<DeleteResponse> {
                    override fun onResponse(response: DeleteResponse) {
                        if (response.result == DocWriteResponse.Result.DELETED) {
                            logger.info(
                                "One time notification policy for task: {} has been removed", taskId
                            )
                        }
                    }

                    override fun onFailure(e: Exception) {
                        logger.info("Remove one time notification policy failed", e)
                    }
                }
            )
        }
    }

    fun getQualifiedChannels(
        lronConfigResponse: GetLRONConfigResponse,
        result: OperationResult
    ): Set<LRONConfigResponse> {
        val runtimeConfig =
            lronConfigResponse.lronConfigResponses.firstOrNull { it.lronConfig.taskId != null }
        val defaultConfig =
            // operation default
            lronConfigResponse.lronConfigResponses.firstOrNull { it.lronConfig.actionName != null && it.lronConfig.taskId == null }
                // global default
                ?: lronConfigResponse.lronConfigResponses.firstOrNull { it.lronConfig.actionName == null && it.lronConfig.taskId == null }

        val channels = ArrayList<LRONConfigResponse>()
        if (runtimeConfig != null) channels.add(runtimeConfig)
        if (defaultConfig != null) channels.add(defaultConfig)

        return channels.filter { ch ->
            val condition = ch.lronConfig.lronCondition
            condition.success && result == OperationResult.COMPLETE || condition.failure && result != OperationResult.COMPLETE
        }.toSet()
    }

    fun escapeQueryString(query: String): String {
        return query.replace("/", "\\/").replace(":", "\\:")
    }

    companion object {
        val MAX_WAIT_TIME: TimeValue = TimeValue.timeValueHours(1)
        const val COMPLETED = "has completed."
        const val COMPLETED_WITH_ERROR = "has completed with errors. Error details:"
        const val TITLE = "Index Management - Index Operation Complete Notification"
        val DELAY: TimeValue = TimeValue.timeValueSeconds(5)
    }
}
