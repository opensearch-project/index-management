/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.resthandler

import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.rollup.action.delete.DeleteRollupAction
import org.opensearch.indexmanagement.rollup.action.delete.DeleteRollupRequest
import org.opensearch.indexmanagement.util.REFRESH
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestDeleteRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                DELETE, "$ROLLUP_JOBS_BASE_URI/{rollupID}",
                DELETE, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}"
            )
        )
    }

    override fun getName(): String = "opendistro_delete_rollup_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupID = request.param("rollupID")
        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        return RestChannelConsumer { channel ->
            channel.newBuilder()
            val deleteRollupRequest = DeleteRollupRequest(rollupID)
                .setRefreshPolicy(refreshPolicy)
            client.execute(DeleteRollupAction.INSTANCE, deleteRollupRequest, RestToXContentListener(channel))
        }
    }
}
