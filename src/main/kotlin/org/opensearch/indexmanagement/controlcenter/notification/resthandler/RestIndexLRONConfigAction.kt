/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.controlcenter.notification.action.index.IndexLRONConfigAction
import org.opensearch.indexmanagement.controlcenter.notification.action.index.IndexLRONConfigRequest
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.util.getDocID
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.util.DRY_RUN
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestIndexLRONConfigAction : BaseRestHandler() {
    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(RestRequest.Method.POST, IndexManagementPlugin.LRON_BASE_URI),
            RestHandler.Route(RestRequest.Method.PUT, "${IndexManagementPlugin.LRON_BASE_URI}/{id}"),
        )
    }

    override fun getName(): String {
        return "create_lron_config_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val docId = request.param("id")
        val dryRun = request.paramAsBoolean(DRY_RUN, false)
        val xcp = request.contentParser()
        val lronConfig = xcp.parseWithType(parse = LRONConfig.Companion::parse)
        val isUpdate = null != docId
        if (isUpdate && getDocID(lronConfig.taskId, lronConfig.actionName) != docId) {
            throw IllegalArgumentException("docId isn't match with lron_config")
        }

        val indexLRONConfigRequest = IndexLRONConfigRequest(lronConfig, isUpdate, dryRun)

        return RestChannelConsumer { channel ->
            client.execute(IndexLRONConfigAction.INSTANCE, indexLRONConfigRequest, RestToXContentListener(channel))
        }
    }
}
