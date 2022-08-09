/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.executepolicy.ExecutePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.executepolicy.ExecutePolicyRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestExecutePolicyAction : BaseRestHandler() {
    override fun getName(): String = "execute_policy_action"

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
                ReplacedRoute(
                        POST, "$ISM_BASE_URI/execute",
                        POST, "$LEGACY_ISM_BASE_URI/execute"
                ),
                ReplacedRoute(
                        POST, "$ISM_BASE_URI/execute/{index}",
                        POST, "$LEGACY_ISM_BASE_URI/{index}"
                )
        )
    }

    @Throws(IOException::class)
    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))

        if (indices.isNullOrEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }

        val body = if (request.hasContent()) {
            XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType).v2()
        } else {
            mapOf()
        }

        val executePolicyRequest = ExecutePolicyRequest(indices.toList())

        return RestChannelConsumer { channel ->
            client.execute(ExecutePolicyAction.INSTANCE, executePolicyRequest, RestToXContentListener(channel))
        }
    }
}