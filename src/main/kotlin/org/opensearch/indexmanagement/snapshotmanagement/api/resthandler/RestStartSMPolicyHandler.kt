/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.start.StartSMRequest
import org.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.opensearch.indexmanagement.snapshotmanagement.util.getValidSMPolicyName
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener

class RestStartSMPolicyHandler : BaseRestHandler() {

    private val log = LogManager.getLogger(RestStartSMPolicyHandler::class.java)

    override fun getName(): String {
        return "snapshot_management_start_policy_rest_handler"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(RestRequest.Method.POST, "$SM_POLICIES_URI/{policyName}/_start")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.getValidSMPolicyName()
        log.debug("Start snapshot management policy request received with policy name [$policyName]")

        val indexReq = StartSMRequest(smPolicyNameToDocId(policyName))
        return RestChannelConsumer {
            client.execute(
                SMActions.START_SM_POLICY_ACTION_TYPE,
                indexReq, RestToXContentListener(it)
            )
        }
    }
}
