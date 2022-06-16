/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.opensearch.action.support.WriteRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.opensearch.indexmanagement.snapshotmanagement.util.getValidSMPolicyName
import org.opensearch.indexmanagement.util.IF_PRIMARY_TERM
import org.opensearch.indexmanagement.util.IF_SEQ_NO
import org.opensearch.indexmanagement.util.REFRESH
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestResponse
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import java.time.Instant

abstract class RestBaseIndexSMPolicyHandler : BaseRestHandler() {

    protected fun prepareIndexRequest(request: RestRequest, client: NodeClient, create: Boolean): RestChannelConsumer {
        val policyName = request.getValidSMPolicyName()

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val policy = SMPolicy.parse(xcp, id = smPolicyNameToDocId(policyName), seqNo = seqNo, primaryTerm = primaryTerm)
            .copy(jobLastUpdateTime = Instant.now())

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
}
