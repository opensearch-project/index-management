/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICY_ACTION_TYPE
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICIES_ACTION_TYPE
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPoliciesRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.opensearch.indexmanagement.snapshotmanagement.util.DEFAULT_SM_POLICY_SORT_FIELD
import org.opensearch.indexmanagement.util.getSearchParams
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestToXContentListener

class RestGetSMPolicyHandler : BaseRestHandler() {

    override fun getName(): String {
        return "snapshot_management_get_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, "$SM_POLICIES_URI/{policyName}"),
            Route(GET, "$SM_POLICIES_URI/")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        return if (policyName.isEmpty()) {
            getAllPolicies(request, client)
        } else {
            getSMPolicyByName(client, policyName)
        }
    }

    private fun getSMPolicyByName(client: NodeClient, policyName: String): RestChannelConsumer {
        return RestChannelConsumer {
            client.execute(GET_SM_POLICY_ACTION_TYPE, GetSMPolicyRequest(smPolicyNameToDocId(policyName)), RestToXContentListener(it))
        }
    }

    private fun getAllPolicies(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val searchParams = request.getSearchParams(DEFAULT_SM_POLICY_SORT_FIELD)

        return RestChannelConsumer {
            client.execute(GET_SM_POLICIES_ACTION_TYPE, GetSMPoliciesRequest(searchParams), RestToXContentListener(it))
        }
    }
}
