/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.notification

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.open.OpenIndexAction
import org.opensearch.action.admin.indices.shrink.ResizeAction
import org.opensearch.client.Client
import org.opensearch.cluster.routing.IndexShardRoutingTable
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.index.engine.Engine
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.index.shard.IndexingOperationListener
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import org.opensearch.indexmanagement.notification.model.NotificationConf
import org.opensearch.indexmanagement.opensearchapi.toMap
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.tasks.TaskResult
import java.lang.Exception
import java.lang.IllegalArgumentException
import java.util.StringJoiner

class TaskCompletionListener(
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    val notificationService: NotificationService,
    val scriptService: ScriptService,
    val client: Client,
) : IndexingOperationListener,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("TaskCompletionListener")) {

    private val logger = LogManager.getLogger(javaClass)
    override fun postIndex(shardId: ShardId, index: Engine.Index, result: Engine.IndexResult) {
        if (result.resultType == Engine.Result.Type.FAILURE) {
            logger.info(
                "Indexing failed for job {} on index {}",
                index.id(),
                shardId.indexName,
            )
            return
        }

        // make sure notification only happens exactly 1 time, we check shardId is primary shard
        val localNodeId: String = clusterService.localNode().id
        val routingTable: IndexShardRoutingTable = clusterService.state().routingTable().shardRoutingTable(shardId)
        var shardNodeId: String? = null

        for (shardRouting in routingTable) {
            // TODO relocating may cause duplicate?
            if (shardRouting.active() && shardRouting.primary()) {
                shardNodeId = shardRouting.currentNodeId()
            }
        }

        if (localNodeId != shardNodeId) {
            logger.debug(
                "Indexing for {} on primary shard is not happened at current node {}",
                index.id(),
                localNodeId,
            )
            return
        }

        // send out the notification
        val parser = XContentHelper.createParser(
            xContentRegistry,
            LoggingDeprecationHandler.INSTANCE,
            index.source(),
            XContentType.JSON,
        )

        val taskResult = TaskResult.PARSER.apply(parser, null)

        if (taskResult.isCompleted == false) {
            // this should not happen
            logger.info("Task {} is not completed!", index.id())
            return
        }

        val taskInfo = taskResult.task

        try {
            launch {
                // TODO get notification config for specific task id, action or default configuration
                val channelConf = notificationService.getNotificationConfByAction(index.id(), taskInfo.action)
                if (channelConf == null) {
                    logger.info("Notification channel is not found for task {}, action {}", taskInfo.id, taskInfo.action)
                    return@launch
                }

                var title = "${taskInfo.description} has completed"
                if (taskResult.errorAsMap.isNotEmpty() || (taskResult.responseAsMap["failures"] as List<*>).isNotEmpty()) {
                    title += " with failures"
                }

                val eventSource = EventSource(title, index.id(), SeverityType.INFO)

                Channel(channelConf.channelId).sendNotification(
                    client,
                    eventSource,
                    buildNotificationMessage(taskResult, channelConf),
                    null,
                )

                // TODO remove

                logger.info("Send out notification for {} done", taskInfo.action)
            }
        } catch (e: Exception) {
            logger.error("Send out notification for action ${taskInfo.action} failed", e)
        }
    }

    private fun buildNotificationMessage(taskResult: TaskResult, channelConf: NotificationConf?): String {
        val task = taskResult.task
        val script = channelConf?.template
        if (script != null) {
            val contextMap = taskResult.task.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)
                .toMap()

            return scriptService.compile(script, TemplateScript.CONTEXT)
                .newInstance(script.params + mapOf("ctx" to contextMap))
                .execute()
        } else {
            return when (task.action) {
                ReindexAction.NAME -> {
                    return StringJoiner(" | ")
                        .add(taskResult.response?.utf8ToString())
                        .add(taskResult.error?.utf8ToString())
                        .toString()
                }

                ResizeAction.NAME -> ""
                org.opensearch.action.admin.indices.forcemerge.ForceMergeAction.NAME -> ""
                OpenIndexAction.NAME -> ""
                else -> {
                    throw IllegalArgumentException("${task.action} is not support for sending out notification")
                }
            }
        }
    }
}
