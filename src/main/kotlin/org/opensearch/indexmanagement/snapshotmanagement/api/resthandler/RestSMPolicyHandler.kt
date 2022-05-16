/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_BASE_URI
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.DELETE_SM_ACTION_TYPE
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_ACTION_TYPE
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete.DeleteSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestRequest.Method.PUT
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestToXContentListener

class RestSMPolicyHandler : BaseRestHandler() {

    private val log = LogManager.getLogger(javaClass)

    override fun getName(): String {
        return "snapshot_management_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, "$SM_BASE_URI/{policyName}"),
            Route(PUT, "$SM_BASE_URI/{policyName}"),
            Route(GET, "$SM_BASE_URI/{policyName}"),
            Route(DELETE, "$SM_BASE_URI/{policyName}"),
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        return when (request.method()) {
            POST -> indexRequest(request, client, true)
            PUT -> indexRequest(request, client, false)
            GET -> getRequest(request, client)
            DELETE -> deleteRequest(request, client)
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
                GET_SM_ACTION_TYPE,
                GetSMPolicyRequest(policyName),
                RestToXContentListener(it)
            )
        }
    }

    private fun indexRequest(request: RestRequest, client: NodeClient, create: Boolean): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        if (policyName == "") {
            throw IllegalArgumentException("Missing policy name")
        }
        // TODO validate policy name validateGeneratedSnapshotName

        log.info("sm dev: receive request ${request.requiredContent().utf8ToString()}")

        val xcp = request.contentParser()
        val policy = SMPolicy.parse(xcp, id = smPolicyNameToDocId(policyName))
        log.info("sm dev: policy parsed $policy")

        return RestChannelConsumer {
            client.execute(
                SMActions.INDEX_SM_ACTION_TYPE,
                IndexSMPolicyRequest(policy, create),
                RestToXContentListener(it)
            )
        }
    }

    private fun deleteRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        if (policyName == "") {
            throw IllegalArgumentException("Missing policy name")
        }

        return RestChannelConsumer {
            client.execute(
                DELETE_SM_ACTION_TYPE,
                DeleteSMPolicyRequest(policyName),
                RestToXContentListener(it)
            )
        }
    }
}
