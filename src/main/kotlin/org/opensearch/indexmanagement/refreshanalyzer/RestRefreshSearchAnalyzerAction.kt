/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.refreshanalyzer

import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.OPEN_DISTRO_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.PLUGINS_BASE_URI
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestRefreshSearchAnalyzerAction : BaseRestHandler() {

    override fun getName(): String = "refresh_search_analyzer_action"

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, REFRESH_SEARCH_ANALYZER_BASE_URI,
                POST, LEGACY_REFRESH_SEARCH_ANALYZER_BASE_URI
            ),
            ReplacedRoute(
                POST, "$REFRESH_SEARCH_ANALYZER_BASE_URI/{index}",
                POST, "$LEGACY_REFRESH_SEARCH_ANALYZER_BASE_URI/{index}"
            )
        )
    }

    // TODO: Add indicesOptions?

    @Throws(IOException::class)
    @Suppress("SpreadOperator")
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))

        if (indices.isNullOrEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val refreshSearchAnalyzerRequest: RefreshSearchAnalyzerRequest = RefreshSearchAnalyzerRequest()
            .indices(*indices)

        return RestChannelConsumer { channel ->
            client.execute(RefreshSearchAnalyzerAction.INSTANCE, refreshSearchAnalyzerRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val REFRESH_SEARCH_ANALYZER_BASE_URI = "$PLUGINS_BASE_URI/_refresh_search_analyzers"
        const val LEGACY_REFRESH_SEARCH_ANALYZER_BASE_URI = "$OPEN_DISTRO_BASE_URI/_refresh_search_analyzers"
    }
}
