/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.action.support.WriteRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_POLICY_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.ALLOW_LIST
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyRequest
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyResponse
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.util.IF_PRIMARY_TERM
import org.opensearch.indexmanagement.util.IF_SEQ_NO
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.indexmanagement.util.REFRESH
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.PUT
import org.opensearch.rest.RestResponse
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

class RestIndexPolicyAction(
    settings: Settings,
    val clusterService: ClusterService
) : BaseRestHandler() {

    @Volatile private var allowList = ALLOW_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
    }

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                PUT, POLICY_BASE_URI,
                PUT, LEGACY_POLICY_BASE_URI
            ),
            ReplacedRoute(
                PUT, "$POLICY_BASE_URI/{policyID}",
                PUT, "$LEGACY_POLICY_BASE_URI/{policyID}"
            )
        )
    }

    override fun getName(): String {
        return "index_policy_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("policyID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing policy ID")
        }

        val xcp = request.contentParser()
        val policy = xcp.parseWithType(id = id, parse = Policy.Companion::parse).copy(lastUpdatedTime = Instant.now())
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        val disallowedActions = policy.getDisallowedActions(allowList)
        if (disallowedActions.isNotEmpty()) {
            return RestChannelConsumer { channel ->
                channel.sendResponse(
                    BytesRestResponse(
                        RestStatus.FORBIDDEN,
                        "You have actions that are not allowed in your policy $disallowedActions"
                    )
                )
            }
        }

        val indexPolicyRequest = IndexPolicyRequest(id, policy, seqNo, primaryTerm, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(
                IndexPolicyAction.INSTANCE, indexPolicyRequest,
                object : RestResponseListener<IndexPolicyResponse>(channel) {
                    override fun buildResponse(response: IndexPolicyResponse): RestResponse {
                        val restResponse = BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                        if (response.status == RestStatus.CREATED) {
                            val location = "$POLICY_BASE_URI/${response.id}"
                            restResponse.addHeader("Location", location)
                        }
                        return restResponse
                    }
                }
            )
        }
    }
}
