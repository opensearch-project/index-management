/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:JvmName("LRONUtils")
package org.opensearch.indexmanagement.controlcenter.notification.util

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.forcemerge.ForceMergeAction
import org.opensearch.action.admin.indices.open.OpenIndexAction
import org.opensearch.action.admin.indices.shrink.ResizeAction
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.client.node.NodeClient
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.controlcenter.notification.LRONConfigResponse
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.TaskId

const val LRON_DEFAULT_ID = "default"
const val LRON_DOC_ID_PREFIX = "LRON:"

const val WITH_PRIORITY = "with_priority"
const val PRIORITY_TASK_ID = 300
const val PRIORITY_DEFAULT_ACTION = 200
const val PRIORITY_DEFAULT = 100
const val DEFAULT_LRON_CONFIG_SORT_FIELD = "lron_config.priority"

val supportedActions = setOf(
    ReindexAction.NAME,
    ResizeAction.NAME,
    ForceMergeAction.NAME,
    OpenIndexAction.NAME
)

fun validateActionName(actionName: String?): Boolean {
    if (null != actionName && !supportedActions.contains(actionName)) {
        return false
    }
    return true
}

fun getPriority(taskId: TaskId? = null, actionName: String? = null): Int {
    return when {
        null != taskId -> PRIORITY_TASK_ID
        null != actionName -> PRIORITY_DEFAULT_ACTION
        else -> PRIORITY_DEFAULT
    }
}

fun getDocID(taskId: TaskId? = null, actionName: String? = null): String {
    val id = taskId?.toString() ?: actionName ?: LRON_DEFAULT_ID
    return LRON_DOC_ID_PREFIX + id
}

fun getLRONConfigAndParse(
    client: NodeClient,
    docId: String,
    xContentRegistry: NamedXContentRegistry,
    actionListener: ActionListener<LRONConfigResponse>
) {
    val getRequest = GetRequest(IndexManagementPlugin.CONTROL_CENTER_INDEX, docId)
    client.get(
        getRequest,
        object : ActionListener<GetResponse> {
            override fun onResponse(response: GetResponse) {
                if (!response.isExists) {
                    actionListener.onFailure(
                        OpenSearchStatusException(
                            "lronConfig $docId not found",
                            RestStatus.NOT_FOUND
                        )
                    )
                    return
                }

                val lronConfig: LRONConfig
                try {
                    lronConfig =
                        parseFromGetResponse(response, xContentRegistry, LRONConfig.Companion::parse)
                } catch (e: IllegalArgumentException) {
                    actionListener.onFailure(e)
                    return
                }
                actionListener.onResponse(
                    LRONConfigResponse(
                        id = response.id,
                        version = response.version,
                        primaryTerm = response.primaryTerm,
                        seqNo = response.seqNo,
                        lronConfig = lronConfig
                    )
                )
            }

            override fun onFailure(t: Exception) {
                actionListener.onFailure(t)
            }
        }
    )
}
