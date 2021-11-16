/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestToXContentListener

class RestExplainRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, "$ROLLUP_JOBS_BASE_URI/{rollupID}/_explain",
                GET, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}/_explain"
            )
        )
    }

    override fun getName(): String = "opendistro_explain_rollup_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupIDs: List<String> = Strings.splitStringByCommaToArray(request.param("rollupID")).toList()
        if (rollupIDs.isEmpty()) {
            throw IllegalArgumentException("Missing rollupID")
        }
        val explainRequest = ExplainRollupRequest(rollupIDs)
        return RestChannelConsumer { channel ->
            client.execute(ExplainRollupAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
