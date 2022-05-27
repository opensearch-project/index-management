/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestToXContentListener

class RestExplainSMPolicyHandler : BaseRestHandler() {

    private val log = LogManager.getLogger(RestExplainSMPolicyHandler::class.java)

    override fun getName(): String {
        return "snapshot_management_explain_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, "$SM_POLICIES_URI/{policyName}/explain")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        return when (request.method()) {
            GET -> getRequest(request, client)
            else -> RestChannelConsumer {
                it.sendResponse(
                    BytesRestResponse(
                        RestStatus.METHOD_NOT_ALLOWED,
                        "${request.method()} is not allowed"
                    )
                )
            }
        }
    }

    private fun getRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        if (policyName == "") {
            throw IllegalArgumentException("Missing policy name")
        }

        return RestChannelConsumer {
            client.execute(
                SMActions.EXPLAIN_SM_ACTION_TYPE,
                ExplainSMPolicyRequest(policyName),
                RestToXContentListener(it)
            )
        }
    }
}
