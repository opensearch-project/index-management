/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.opensearch.action.support.WriteRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete.DeleteSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.opensearch.indexmanagement.util.REFRESH
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener

class RestDeleteSMPolicyHandler : BaseRestHandler() {

    override fun getName(): String {
        return "snapshot_management_delete_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(RestRequest.Method.DELETE, "$SM_POLICIES_URI/{policyName}")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
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
                SMActions.DELETE_SM_POLICY_ACTION_TYPE,
                DeleteSMPolicyRequest(smPolicyNameToDocId(policyName)).setRefreshPolicy(refreshPolicy),
                RestToXContentListener(it)
            )
        }
    }
}
