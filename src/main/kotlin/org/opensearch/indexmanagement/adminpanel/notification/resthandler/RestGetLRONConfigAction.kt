/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.resthandler
import GetLRONConfigsRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.action.get.GetLRONConfigAction
import org.opensearch.indexmanagement.common.model.rest.DEFAULT_PAGINATION_FROM
import org.opensearch.indexmanagement.common.model.rest.DEFAULT_PAGINATION_SIZE
import org.opensearch.indexmanagement.common.model.rest.DEFAULT_QUERY_STRING
import org.opensearch.indexmanagement.common.model.rest.DEFAULT_SORT_ORDER
import org.opensearch.indexmanagement.common.model.rest.SearchParams
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestGetLRONConfigAction : BaseRestHandler() {
    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(RestRequest.Method.GET, IndexManagementPlugin.LRON_BASE_URI),
            RestHandler.Route(RestRequest.Method.GET, "${IndexManagementPlugin.LRON_BASE_URI}/{id}")
        )
    }

    override fun getName(): String {
        return "get_lron_config_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val ids = Strings.splitStringByCommaToArray(request.param("id"))
        // val includeDefault = request.paramAsBoolean("include_default", false)

        val searchParams = SearchParams(
            DEFAULT_PAGINATION_SIZE, DEFAULT_PAGINATION_FROM,
            "lron_config.priority", DEFAULT_SORT_ORDER, DEFAULT_QUERY_STRING
        )
        val getLRONConfigRequest = GetLRONConfigsRequest(searchParams, ids)

        return RestChannelConsumer { channel ->
            client.execute(GetLRONConfigAction.INSTANCE, getLRONConfigRequest, RestToXContentListener(channel))
        }
    }
}
