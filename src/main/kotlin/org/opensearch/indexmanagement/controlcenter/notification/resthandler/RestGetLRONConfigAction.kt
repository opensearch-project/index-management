/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.resthandler
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.controlcenter.notification.action.get.GetLRONConfigAction
import org.opensearch.indexmanagement.controlcenter.notification.action.get.GetLRONConfigRequest
import org.opensearch.indexmanagement.controlcenter.notification.util.DEFAULT_LRON_CONFIG_SORT_FIELD
import org.opensearch.indexmanagement.util.getSearchParams
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestGetLRONConfigAction : BaseRestHandler() {
    override fun routes(): List<RestHandler.Route> = listOf(
        RestHandler.Route(RestRequest.Method.GET, IndexManagementPlugin.LRON_BASE_URI),
        RestHandler.Route(RestRequest.Method.GET, "${IndexManagementPlugin.LRON_BASE_URI}/{id}"),
    )

    override fun getName(): String = "get_lron_config_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val docId = request.param("id")
        val searchParams = if (null == docId) request.getSearchParams(DEFAULT_LRON_CONFIG_SORT_FIELD) else null
        val getLRONConfigRequest = GetLRONConfigRequest(docId, searchParams)

        return RestChannelConsumer { channel ->
            client.execute(GetLRONConfigAction.INSTANCE, getLRONConfigRequest, RestToXContentListener(channel))
        }
    }
}
