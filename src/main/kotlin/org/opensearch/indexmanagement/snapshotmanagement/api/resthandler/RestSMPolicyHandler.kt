/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_BASE_URI
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMRequest
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestToXContentListener

class RestSMPolicyHandler : BaseRestHandler() {

    private val log = LogManager.getLogger(RestSMPolicyHandler::class.java)

    override fun getName(): String {
        return "snapshot_management_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, "$SM_BASE_URI/{policyName}"),
            Route(GET, "$SM_BASE_URI/{policyName}")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        return when (request.method()) {
            POST -> postRequest(request, client)
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

    // private fun getRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
    //     val policyName = request.param("policyName", "")
    // }

    private fun postRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        if (policyName == "") {
            throw IllegalArgumentException("Missing policy name")
        }
        // TODO validate policy name validateGeneratedSnapshotName

        log.info("sm receive request ${request.requiredContent().utf8ToString()}")

        val xcp = request.contentParser()
        val smPolicy = SMPolicy.parse(xcp, policyName = policyName)
        log.info("sm parsed $smPolicy")

        val indexReq = IndexSMRequest(smPolicy)
        return RestChannelConsumer {
            client.execute(
                SMActions.INDEX_SM_ACTION_TYPE,
                indexReq, RestToXContentListener(it)
            )
        }
    }
}
