/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.execute.ExecuteSMRequest
import org.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.PUT
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestToXContentListener

class RestExecuteSMPolicyHandler : BaseRestHandler() {

    private val log = LogManager.getLogger(RestExecuteSMPolicyHandler::class.java)

    override fun getName(): String {
        return "snapshot_management_execute_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(PUT, "$SM_POLICIES_URI/{policyName}/_execute")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        return when (request.method()) {
            PUT -> putRequest(request, client)
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

    private fun putRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        if (policyName == "") {
            throw IllegalArgumentException("Missing policy name")
        }

        val executeReq = ExecuteSMRequest(smPolicyNameToDocId(policyName))
        return RestChannelConsumer {
            client.execute(
                SMActions.EXECUTE_SM_ACTION_TYPE,
                executeReq, RestToXContentListener(it)
            )
        }
    }
}
