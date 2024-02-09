/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest

class RestUpdateSMPolicyHandler : RestBaseIndexSMPolicyHandler() {

    override fun getName(): String {
        return "snapshot_management_update_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(RestRequest.Method.PUT, "$SM_POLICIES_URI/{policyName}"),
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer = prepareIndexRequest(request, client, false)
}
