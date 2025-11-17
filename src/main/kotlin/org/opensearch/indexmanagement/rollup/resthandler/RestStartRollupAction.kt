/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.resthandler

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.rollup.action.start.StartRollupAction
import org.opensearch.indexmanagement.rollup.action.start.StartRollupRequest
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

class RestStartRollupAction : BaseRestHandler() {
    override fun routes(): List<Route> = emptyList()

    override fun replacedRoutes(): List<ReplacedRoute> = listOf(
        ReplacedRoute(
            POST, "$ROLLUP_JOBS_BASE_URI/{rollupID}/_start",
            POST, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}/_start",
        ),
    )

    override fun getName(): String = "opendistro_start_rollup_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("rollupID", NO_ID)
        val startRequest = StartRollupRequest(id)
        return RestChannelConsumer { channel ->
            client.execute(StartRollupAction.INSTANCE, startRequest, RestToXContentListener(channel))
        }
    }
}
