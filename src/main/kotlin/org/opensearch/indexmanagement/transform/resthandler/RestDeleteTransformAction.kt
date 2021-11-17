/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.transform.action.delete.DeleteTransformsAction
import org.opensearch.indexmanagement.transform.action.delete.DeleteTransformsRequest
import org.opensearch.indexmanagement.transform.action.delete.DeleteTransformsRequest.Companion.DEFAULT_FORCE_DELETE
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.DELETE
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestDeleteTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            Route(DELETE, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String = "opendistro_delete_transform_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformID = request.param("transformID")
        val force = request.paramAsBoolean("force", DEFAULT_FORCE_DELETE)
        return RestChannelConsumer { channel ->
            channel.newBuilder()
            val deleteTransformsRequest = DeleteTransformsRequest(transformID.split(","), force)
            client.execute(DeleteTransformsAction.INSTANCE, deleteTransformsRequest, RestToXContentListener(channel))
        }
    }
}
