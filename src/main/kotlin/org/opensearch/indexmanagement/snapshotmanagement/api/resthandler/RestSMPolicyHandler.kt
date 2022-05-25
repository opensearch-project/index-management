/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.DELETE_SM_POLICY_ACTION_TYPE
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICY_ACTION_TYPE
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete.DeleteSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.util.IF_PRIMARY_TERM
import org.opensearch.indexmanagement.util.IF_SEQ_NO
import org.opensearch.indexmanagement.util.REFRESH
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestRequest.Method.PUT
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.RestResponse
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import org.opensearch.rest.action.RestToXContentListener
import java.time.Instant

class RestSMPolicyHandler : BaseRestHandler() {

    private val log = LogManager.getLogger(javaClass)

    override fun getName(): String {
        return "snapshot_management_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(POST, "$SM_POLICIES_URI/{policyName}"),
            Route(PUT, "$SM_POLICIES_URI/{policyName}"),
            Route(GET, "$SM_POLICIES_URI/{policyName}"),
            Route(DELETE, "$SM_POLICIES_URI/{policyName}"),
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
                GET_SM_POLICY_ACTION_TYPE,
                GetSMPolicyRequest(smPolicyNameToDocId(policyName)),
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

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val policy = SMPolicy.parse(xcp, id = smPolicyNameToDocId(policyName), seqNo = seqNo, primaryTerm = primaryTerm)
            .copy(jobLastUpdateTime = Instant.now())
        log.info("sm dev: policy parsed $policy")

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        return RestChannelConsumer {
            client.execute(
                SMActions.INDEX_SM_POLICY_ACTION_TYPE,
                IndexSMPolicyRequest(policy, create, refreshPolicy),
                object : RestResponseListener<IndexSMPolicyResponse>(it) {
                    override fun buildResponse(response: IndexSMPolicyResponse): RestResponse {
                        val restResponse = BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                        if (response.status == RestStatus.CREATED || response.status == RestStatus.OK) {
                            val location = "$SM_POLICIES_URI/${response.policy.policyName}"
                            restResponse.addHeader("Location", location)
                        }
                        return restResponse
                    }
                }
            )
        }
    }

    private fun deleteRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        if (policyName == "") {
            throw IllegalArgumentException("Missing policy name")
        }

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        return RestChannelConsumer {
            client.execute(
                DELETE_SM_POLICY_ACTION_TYPE,
                DeleteSMPolicyRequest(smPolicyNameToDocId(policyName)).setRefreshPolicy(refreshPolicy),
                RestToXContentListener(it)
            )
        }
    }
}
