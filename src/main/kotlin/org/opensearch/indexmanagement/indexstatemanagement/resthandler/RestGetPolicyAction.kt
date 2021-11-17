/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_POLICY_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.model.SearchParams
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesRequest
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_PAGINATION_FROM
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_PAGINATION_SIZE
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_POLICY_SORT_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_QUERY_STRING
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_SORT_ORDER
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.HEAD
import org.opensearch.rest.action.RestActions
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext

private val log = LogManager.getLogger(RestGetPolicyAction::class.java)

class RestGetPolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, POLICY_BASE_URI,
                GET, LEGACY_POLICY_BASE_URI
            ),
            ReplacedRoute(
                GET, "$POLICY_BASE_URI/{policyID}",
                GET, "$LEGACY_POLICY_BASE_URI/{policyID}"
            ),
            ReplacedRoute(
                HEAD, "$POLICY_BASE_URI/{policyID}",
                HEAD, "$LEGACY_POLICY_BASE_URI/{policyID}"
            )
        )
    }

    override fun getName(): String {
        return "get_policy_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${request.path()}")

        val policyId = request.param("policyID")

        var fetchSrcContext: FetchSourceContext = FetchSourceContext.FETCH_SOURCE
        if (request.method() == HEAD) {
            fetchSrcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }

        val size = request.paramAsInt("size", DEFAULT_PAGINATION_SIZE)
        val from = request.paramAsInt("from", DEFAULT_PAGINATION_FROM)
        val sortField = request.param("sortField", DEFAULT_POLICY_SORT_FIELD)
        val sortOrder = request.param("sortOrder", DEFAULT_SORT_ORDER)
        val queryString = request.param("queryString", DEFAULT_QUERY_STRING)

        return RestChannelConsumer { channel ->
            if (policyId == null || policyId.isEmpty()) {
                val getPoliciesRequest = GetPoliciesRequest(SearchParams(size, from, sortField, sortOrder, queryString))
                client.execute(GetPoliciesAction.INSTANCE, getPoliciesRequest, RestToXContentListener(channel))
            } else {
                val getPolicyRequest = GetPolicyRequest(policyId, RestActions.parseVersion(request), fetchSrcContext)
                client.execute(GetPolicyAction.INSTANCE, getPolicyRequest, RestToXContentListener(channel))
            }
        }
    }
}
