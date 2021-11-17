/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_POLICY_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyRequest
import org.opensearch.indexmanagement.util.REFRESH
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.action.RestStatusToXContentListener
import java.io.IOException

class RestDeletePolicyAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                DELETE, "$POLICY_BASE_URI/{policyID}",
                DELETE, "$LEGACY_POLICY_BASE_URI/{policyID}"
            )
        )
    }

    override fun getName(): String = "delete_policy_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyId = request.param("policyID")
        if (policyId == null || policyId.isEmpty()) {
            throw IllegalArgumentException("Missing policy ID")
        }

        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        val deletePolicyRequest = DeletePolicyRequest(policyId, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(DeletePolicyAction.INSTANCE, deletePolicyRequest, RestStatusToXContentListener(channel))
        }
    }
}
