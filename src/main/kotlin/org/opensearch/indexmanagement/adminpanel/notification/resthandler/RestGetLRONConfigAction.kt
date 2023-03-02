/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.resthandler
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigAction
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestRequest
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.rest.RestHandler
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestGetLRONConfigAction : BaseRestHandler() {
    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(RestRequest.Method.GET, IndexManagementPlugin.LRON_BASE_URI),
            RestHandler.Route(RestRequest.Method.GET, "${IndexManagementPlugin.LRON_BASE_URI}/{taskID}")
        )
    }

    override fun getName(): String {
        return "get_lron_config_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val taskID = request.param("taskID", NO_ID)
        val includeDefault = request.paramAsBoolean("include_default", false)

        val getLRONConfigRequest = GetLRONConfigRequest(taskID, includeDefault)

        return RestChannelConsumer { channel ->
            client.execute(GetLRONConfigAction.INSTANCE, getLRONConfigRequest, RestToXContentListener(channel))
        }
    }
}
