/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.index

import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_BASE_URI
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener

class RestIndexSMAction : BaseRestHandler() {

    private val log = LogManager.getLogger(javaClass)

    override fun getName(): String {
        return "sm_index_policy_action"
    }

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(
                RestRequest.Method.PUT, "$SM_BASE_URI/{policyName}"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val policyName = request.param("policyName", "")
        if (policyName == "") {
            throw IllegalArgumentException("Missing policy name")
        }

        val xcp = request.contentParser()
        log.info("sm receive request ${request.requiredContent().utf8ToString()}")
        val smPolicy = SMPolicy.parse(xcp, policyName = policyName)
        log.info("sm parsed $smPolicy")

        val indexReq = IndexSMRequest(smPolicy)

        return RestChannelConsumer {
            client.execute(IndexSMAction.INSTANCE, indexReq, RestToXContentListener(it))
        }
    }
}
