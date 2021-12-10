/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.transform.action.preview.PreviewTransformAction
import org.opensearch.indexmanagement.transform.action.preview.PreviewTransformRequest
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener

class RestPreviewTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(POST, TRANSFORM_BASE_URI),
            RestHandler.Route(POST, "$TRANSFORM_BASE_URI/_preview")
        )
    }

    override fun getName(): String {
        return "opendistro_preview_transform_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val xcp = request.contentParser()
        val transform = xcp.parseWithType(parse = Transform.Companion::parse)
        val previewTransformRequest = PreviewTransformRequest(transform)
        return RestChannelConsumer { channel ->
            client.execute(PreviewTransformAction.INSTANCE, previewTransformRequest, RestToXContentListener(channel))
        }
    }
}
