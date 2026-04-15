/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.core.common.Strings
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.SimulatePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.SimulatePolicyRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient

/**
 * REST handler for the ISM policy simulate endpoint.
 *
 * POST /_plugins/_ism/simulate
 *
 * Request body:
 * {
 *   "policy_id": "my-policy",    // use an existing policy by ID  — OR —
 *   "policy": { ... },           // supply an inline policy definition
 *   "indices": ["index1", "index2"]
 * }
 *
 * Exactly one of `policy_id` or `policy` must be provided.
 */
class RestSimulatePolicyAction : BaseRestHandler() {
    companion object {
        const val SIMULATE_BASE_URI = "$ISM_BASE_URI/simulate"
        const val LEGACY_SIMULATE_BASE_URI = "$LEGACY_ISM_BASE_URI/simulate"

        const val POLICY_ID_FIELD = "policy_id"
        const val POLICY_FIELD = "policy"
        const val INDICES_FIELD = "indices"
    }

    override fun getName(): String = "ism_simulate_policy_action"

    override fun routes(): List<Route> = emptyList()

    override fun replacedRoutes(): List<ReplacedRoute> = listOf(
        ReplacedRoute(
            POST, SIMULATE_BASE_URI,
            POST, LEGACY_SIMULATE_BASE_URI,
        ),
    )

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val xcp = request.contentParser()
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)

        var policyId: String? = null
        var policy: Policy? = null
        val indices = mutableListOf<String>()

        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()
            when (fieldName) {
                POLICY_ID_FIELD -> policyId = xcp.text()

                POLICY_FIELD -> {
                    ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                    policy = Policy.parse(xcp)
                }

                INDICES_FIELD -> {
                    ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                    while (xcp.nextToken() != Token.END_ARRAY) {
                        indices.add(xcp.text())
                    }
                }

                else -> xcp.skipChildren()
            }
        }

        val simulateRequest = SimulatePolicyRequest(
            indices = indices,
            policyId = policyId,
            policy = policy,
        )

        return RestChannelConsumer { channel ->
            client.execute(SimulatePolicyAction.INSTANCE, simulateRequest, RestToXContentListener(channel))
        }
    }
}
