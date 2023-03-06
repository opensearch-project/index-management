/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filter

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
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.opensearch.action.admin.indices.shrink.ResizeAction
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.admin.indices.shrink.ResizeResponse
import org.opensearch.action.admin.indices.shrink.ResizeType
import org.opensearch.action.support.ActiveShardCount
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigsAction
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigsRequest
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigsResponse
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.util.getDocID
import org.opensearch.indexmanagement.common.model.rest.DEFAULT_PAGINATION_SIZE
import org.opensearch.indexmanagement.common.model.rest.SORT_ORDER_DESC
import org.opensearch.indexmanagement.common.model.rest.SearchParams
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.tasks.Task
import org.opensearch.tasks.TaskId
import java.lang.StringBuilder

@Suppress("LongParameterList", "MaxLineLength")
class NotificationActionListener<Request : ActionRequest, Response : ActionResponse>(
    val delegate: ActionListener<Response>,
    val client: Client,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    val action: String,
    val task: Task,
    val scriptService: ScriptService,
    val activeShardsObserver: ActiveShardsObserver,
    val request: Request
) : ActionListener<Response>,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("NotificationActionListener")) {

    var resizeType: ResizeType? = null

    private val logger = LogManager.getLogger(NotificationActionListener::class.java)

    override fun onResponse(response: Response) {
        try {
            delegate.onResponse(response)
            launch {
                postOnResponse(response)
            }
        } catch (e: Exception) {
            onFailure(e)
        }
    }

    override fun onFailure(e: Exception) {
        try {
            delegate.onFailure(e)
        } finally {
            notifyOperationComplete(action, e = e)
        }
    }

    private suspend fun postOnResponse(response: Response) {
        try {
            if (response is ResizeResponse) {
                // Not all shards are started
                if (response.isShardsAcknowledged == false) {
                    logger.debug("Not all shards are started, continue monitoring on shards status")
                    activeShardsObserver.waitForActiveShards(
                        arrayOf<String>(response.index()),
                        ActiveShardCount.DEFAULT, // once all primary shards are started, we think it is completed
                        MAX_WAIT_TIME,
                        { shardsAcknowledged: Boolean ->
                            if (shardsAcknowledged == false) {
                                notifyOperationComplete(action, isTimeout = true)
                            } else {
                                notifyOperationComplete(
                                    action,
                                    response = ResizeResponse(
                                        response.isAcknowledged,
                                        shardsAcknowledged,
                                        response.index()
                                    )
                                )
                            }
                        }, { e: java.lang.Exception ->
                        // failed
                        notifyOperationComplete(action, e = e)
                    }
                    )
                } else {
                    notifyOperationComplete(action, response = response)
                }
            } else {
                // other operations are not waiting for shards started
                notifyOperationComplete(action, response = response)
            }
        } catch (e: Exception) {
            // ignore
        }
    }

    private fun notifyOperationComplete(
        action: String,
        response: ActionResponse? = null,
        e: Exception? = null,
        isTimeout: Boolean = false
    ) {
        val taskId = TaskId(clusterService.localNode().id, task.id).toString()
        val queryString = "_id:(\"${getDocID()}\" OR \"${getDocID(taskId = taskId)}\" OR \"${getDocID(actionName = action)}\")"
        val searchParam = SearchParams(
            DEFAULT_PAGINATION_SIZE, 0, "lron_config.priority", SORT_ORDER_DESC, queryString
        )
        client.execute(
            GetLRONConfigsAction.INSTANCE,
            GetLRONConfigsRequest(searchParam),
            object : ActionListener<GetLRONConfigsResponse> {
                override fun onResponse(lronConfigsResponse: GetLRONConfigsResponse) {
                    if (0 == lronConfigsResponse.totalNumber) {
                        logger.info("No notification channel configured for task: {} action: {}", taskId.toString(), action)
                        return
                    }
                    lronConfigsResponse.lronConfigResponses.first().let {
                        val title = buildNotificationTitle()
                        val eventSource = EventSource(title, taskId.toString(), SeverityType.INFO)
                        launch {
                            val user = it.lronConfig.user
                            val config = it.lronConfig
                            it.lronConfig.channels?.forEach {
                                try {
                                    it.sendNotification(
                                        client,
                                        eventSource,
                                        buildNotificationMessage(response, e, isTimeout, config),
                                        user,
                                    )
                                } catch (osse: OpenSearchStatusException) {
                                    logger.warn("Sending notification failed, restStatus {}", osse.status(), osse)
                                } catch (e: Exception) {
                                    logger.error("Sending notification failed", e)
                                }
                            }
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    logger.error("Can't get notification channel config for action {}", action, e)
                    // TODO retry
                }
            }
        )
    }

    private fun buildNotificationTitle() = "Index Management - Index Operation Complete Notification"

    fun buildNotificationMessage(
        response: ActionResponse?,
        e: Exception?,
        isTimeout: Boolean,
        lronConfig: LRONConfig
    ): String {
        var result = StringBuilder()

        val script = if (e == null) {
            lronConfig.successMessageTemplate
        } else {
            lronConfig.failedMessageTemplate
        }
        if (script != null) {
            val contextMap = mutableMapOf<String, String>()
            contextMap["action"] = action

            return scriptService.compile(script, TemplateScript.CONTEXT)
                .newInstance(script.params + mapOf("ctx" to contextMap)).execute()
        } else {
            when (action) {
                ReindexAction.NAME -> {
                    assert(response is BulkByScrollResponse)
                    response as BulkByScrollResponse
                    val cancelled = response.reasonCancelled
                    result.append("${task.description} ")
                        .append(
                            if (cancelled.isNullOrBlank() == false) {
                                cancelled
                            } else if (e != null) {
                                COMPLETED_WITH_ERROR
                            } else {
                                COMPLETED
                            }
                        ).append(System.lineSeparator())
                    // "took":8154,"timed_out":false,"total":14074,"updated":0,"created":14074,"deleted":0,
                    result.append("Details: total: ${response.total}, created: ${response.created}, updated: ${response.updated}, deleted: ${response.deleted}")
                }

                ResizeAction.NAME -> {
                    assert(request is ResizeRequest)
                    request as ResizeRequest
                    result.append("${resizeType?.name?.lowercase()} from ${request.sourceIndex} to ${request.targetIndexRequest.index()} ")
                        .append(
                            if (isTimeout == true) {
                                COMPLETED_WITH_TIMEOUT
                            } else if (e != null) {
                                COMPLETED_WITH_ERROR
                            } else {
                                COMPLETED
                            }
                        )
                }

                org.opensearch.action.admin.indices.forcemerge.ForceMergeAction.NAME -> {
                    result.append("force_merge for index [${(request as ForceMergeRequest).indices().joinToString(",")}] $COMPLETED")
                }

                else -> {
                    throw IllegalArgumentException("$action is not support for sending out notification")
                }
            }
            return result.toString()
        }
    }

    public companion object {
        val MAX_WAIT_TIME: TimeValue = TimeValue.timeValueHours(12)
        const val COMPLETED = "has completed."
        const val COMPLETED_WITH_ERROR = "$COMPLETED with errors. Error details: "
        val COMPLETED_WITH_TIMEOUT = "has timeout within ${MAX_WAIT_TIME.toHumanReadableString(0)}"
    }
}
