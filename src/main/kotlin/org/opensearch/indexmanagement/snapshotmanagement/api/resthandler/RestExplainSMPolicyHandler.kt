/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestToXContentListener

class RestExplainSMPolicyHandler : BaseRestHandler() {

    private val log = LogManager.getLogger(RestExplainSMPolicyHandler::class.java)

    override fun getName(): String {
        return "snapshot_management_explain_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, "$SM_POLICIES_URI/{policyName}/_explain")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        var policyNames: Array<String> = Strings.splitStringByCommaToArray(request.param("policyName", ""))
        if (policyNames.isEmpty()) policyNames = arrayOf("*")
        log.debug("Explain snapshot management policy request received with policy name(s) [$policyNames]")

        return RestChannelConsumer {
            client.execute(
                SMActions.EXPLAIN_SM_POLICY_ACTION_TYPE,
                ExplainSMPolicyRequest(policyNames),
                RestToXContentListener(it)
            )
        }
    }
}
