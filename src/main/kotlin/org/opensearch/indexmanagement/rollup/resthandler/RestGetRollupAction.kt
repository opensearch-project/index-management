/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.rollup.action.get.GetRollupAction
import org.opensearch.indexmanagement.rollup.action.get.GetRollupRequest
import org.opensearch.indexmanagement.rollup.action.get.GetRollupsAction
import org.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest
import org.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_FROM
import org.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SEARCH_STRING
import org.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SIZE
import org.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SORT_DIRECTION
import org.opensearch.indexmanagement.rollup.action.get.GetRollupsRequest.Companion.DEFAULT_SORT_FIELD
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.HEAD
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext

class RestGetRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, ROLLUP_JOBS_BASE_URI,
                GET, LEGACY_ROLLUP_JOBS_BASE_URI
            ),
            ReplacedRoute(
                GET, "$ROLLUP_JOBS_BASE_URI/{rollupID}",
                GET, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}"
            ),
            ReplacedRoute(
                HEAD, "$ROLLUP_JOBS_BASE_URI/{rollupID}",
                HEAD, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}"
            )
        )
    }

    override fun getName(): String {
        return "opendistro_get_rollup_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupID = request.param("rollupID")
        val searchString = request.param("search", DEFAULT_SEARCH_STRING)
        val from = request.paramAsInt("from", DEFAULT_FROM)
        val size = request.paramAsInt("size", DEFAULT_SIZE)
        val sortField = request.param("sortField", DEFAULT_SORT_FIELD)
        val sortDirection = request.param("sortDirection", DEFAULT_SORT_DIRECTION)
        return RestChannelConsumer { channel ->
            if (rollupID == null || rollupID.isEmpty()) {
                val req = GetRollupsRequest(
                    searchString,
                    from,
                    size,
                    sortField,
                    sortDirection
                )
                client.execute(GetRollupsAction.INSTANCE, req, RestToXContentListener(channel))
            } else {
                val req = GetRollupRequest(rollupID, if (request.method() == HEAD) FetchSourceContext.DO_NOT_FETCH_SOURCE else null)
                client.execute(GetRollupAction.INSTANCE, req, RestToXContentListener(channel))
            }
        }
    }
}
