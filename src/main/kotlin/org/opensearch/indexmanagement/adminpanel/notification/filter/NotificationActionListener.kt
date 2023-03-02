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
import org.opensearch.action.ActionResponse
import org.opensearch.action.admin.indices.open.OpenIndexAction
import org.opensearch.action.admin.indices.shrink.ResizeAction
import org.opensearch.action.admin.indices.shrink.ResizeType
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigAction
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigRequest
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigResponse
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.notification.NotificationService
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.tasks.TaskId
import java.lang.Exception
import java.lang.IllegalArgumentException

@Suppress("LongParameterList", "MaxLineLength")
class NotificationActionListener<Response : ActionResponse>(
    val delegate: ActionListener<Response>,
    val client: Client,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    val notificationService: NotificationService,
    val action: String,
    val taskId: Long,
    val scriptService: ScriptService
) : ActionListener<Response>,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("NotificationActionListener")) {

    var resizeType: ResizeType? = null

    private val logger = LogManager.getLogger(NotificationActionListener::class.java)

    override fun onResponse(response: Response) {
        try {
            delegate.onResponse(response)
        } catch (e: Exception) {
            delegate.onFailure(e)
        } finally {
            notifyOperationComplete(action, response, null)
        }
    }

    override fun onFailure(e: Exception) {
        try {
            delegate.onFailure(e)
        } finally {
            notifyOperationComplete(action, null, e)
        }
    }

    private fun notifyOperationComplete(action: String, response: Response?, e: Exception?) {
        val taskId = TaskId(clusterService.localNode().id, taskId)
        client.execute(GetLRONConfigAction.INSTANCE,
            GetLRONConfigRequest(), // TODO get by action
            object : ActionListener<GetLRONConfigResponse> {
                override fun onResponse(lronConfigResponse: GetLRONConfigResponse) {
                    lronConfigResponse.lronConfig?.let {
                        val title = buildNotificationTitle()
                        val eventSource = EventSource(title, taskId.toString(), SeverityType.INFO)
                        launch {
                            try {
                                it.channel?.sendNotification(
                                    client,
                                    eventSource,
                                    buildNotificationMessage(action, response, e, it),
                                    it.user,
                                )
                            } catch (osse: OpenSearchStatusException) {
                                logger.warn("Sending notification failed", osse)
                            } catch (e: Exception) {
                                logger.error("Sending notification failed", e)
                            }
                        }
                    }

                }

                override fun onFailure(e: Exception) {
                    logger.error("Can't get notification channel for action {}", action, e)
                }

            })
    }

    private fun buildNotificationTitle(): String {
        var title = ""
        return title;
    }

    fun buildNotificationMessage(action: String, response: Response?, e: Exception?, lronConfig: LRONConfig): String {
        val script = lronConfig?.successMessageTemplate
        if (script != null) {
//            val contextMap = response.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)
//                .toMap()

            val contextMap = mapOf<String, Any>()

            return scriptService.compile(script, TemplateScript.CONTEXT)
                .newInstance(script.params + mapOf("ctx" to contextMap)).execute()
        } else {
            return when (action) {
                ReindexAction.NAME -> {
                    ""
                }

                ResizeAction.NAME -> ""
                org.opensearch.action.admin.indices.forcemerge.ForceMergeAction.NAME -> ""
                OpenIndexAction.NAME -> ""
                else -> {
                    throw IllegalArgumentException("${action} is not support for sending out notification")
                }
            }
        }
    }
}