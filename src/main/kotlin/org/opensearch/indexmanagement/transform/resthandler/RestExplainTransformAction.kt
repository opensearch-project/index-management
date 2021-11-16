/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.transform.action.explain.ExplainTransformAction
import org.opensearch.indexmanagement.transform.action.explain.ExplainTransformRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestToXContentListener

class RestExplainTransformAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(Route(GET, "$TRANSFORM_BASE_URI/{transformID}/_explain"))
    }

    override fun getName(): String = "opendistro_explain_transform_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformIDs: List<String> = Strings.splitStringByCommaToArray(request.param("transformID")).toList()
        if (transformIDs.isEmpty()) {
            throw IllegalArgumentException("Missing transformID")
        }
        val explainRequest = ExplainTransformRequest(transformIDs)
        return RestChannelConsumer { channel ->
            client.execute(ExplainTransformAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
