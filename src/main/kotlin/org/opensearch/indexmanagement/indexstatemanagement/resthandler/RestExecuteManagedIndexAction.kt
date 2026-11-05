/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.executemanagedindex.ExecuteManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.executemanagedindex.ExecuteManagedIndexRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient

class RestExecuteManagedIndexAction : BaseRestHandler() {
    override fun routes(): List<Route> = listOf(
        Route(POST, "$EXECUTE_BASE_URI/{index}"),
    )

    override fun getName(): String = "execute_managed_index"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val index = request.param("index")
        if (index.isNullOrBlank()) {
            throw IllegalArgumentException("Missing index")
        }

        val executeRequest = ExecuteManagedIndexRequest(index)

        return RestChannelConsumer { channel ->
            client.execute(ExecuteManagedIndexAction.INSTANCE, executeRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val EXECUTE_BASE_URI = "$ISM_BASE_URI/execute"
    }
}
