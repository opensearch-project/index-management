/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.opensearch.action.support.WriteRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.transform.action.index.IndexTransformAction
import org.opensearch.indexmanagement.transform.action.index.IndexTransformRequest
import org.opensearch.indexmanagement.transform.action.index.IndexTransformResponse
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.util.IF_PRIMARY_TERM
import org.opensearch.indexmanagement.util.IF_SEQ_NO
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.indexmanagement.util.REFRESH
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.PUT
import org.opensearch.rest.RestResponse
import org.opensearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

class RestIndexTransformAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(PUT, TRANSFORM_BASE_URI),
            RestHandler.Route(PUT, "$TRANSFORM_BASE_URI/{transformID}"),
        )
    }

    override fun getName(): String {
        return "opendistro_index_transform_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("transformID", NO_ID)
        if (NO_ID == id) {
            throw IllegalArgumentException("Missing transform ID")
        }

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val transform = xcp.parseWithType(id = id, seqNo = seqNo, primaryTerm = primaryTerm, parse = Transform.Companion::parse)
            .copy(updatedAt = Instant.now())
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val indexTransformRequest = IndexTransformRequest(transform, refreshPolicy)
        return RestChannelConsumer { channel ->
            client.execute(IndexTransformAction.INSTANCE, indexTransformRequest, indexTransformResponse(channel))
        }
    }

    private fun indexTransformResponse(channel: RestChannel): RestResponseListener<IndexTransformResponse> {
        return object : RestResponseListener<IndexTransformResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexTransformResponse): RestResponse {
                val restResponse =
                    BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (response.status == RestStatus.CREATED) {
                    val location = "$TRANSFORM_BASE_URI/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}
