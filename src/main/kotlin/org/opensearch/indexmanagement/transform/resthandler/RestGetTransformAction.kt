/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.transform.action.get.GetTransformAction
import org.opensearch.indexmanagement.transform.action.get.GetTransformRequest
import org.opensearch.indexmanagement.transform.action.get.GetTransformsAction
import org.opensearch.indexmanagement.transform.action.get.GetTransformsRequest
import org.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_FROM
import org.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SEARCH_STRING
import org.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SIZE
import org.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SORT_DIRECTION
import org.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SORT_FIELD
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.HEAD
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext

class RestGetTransformAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, TRANSFORM_BASE_URI),
            Route(GET, "$TRANSFORM_BASE_URI/{transformID}"),
            Route(HEAD, "$TRANSFORM_BASE_URI/{transformID}")
        )
    }

    override fun getName(): String {
        return "opendistro_get_transform_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val transformID = request.param("transformID")
        val searchString = request.param("search", DEFAULT_SEARCH_STRING)
        val from = request.paramAsInt("from", DEFAULT_FROM)
        val size = request.paramAsInt("size", DEFAULT_SIZE)
        val sortField = request.param("sortField", DEFAULT_SORT_FIELD)
        val sortDirection = request.param("sortDirection", DEFAULT_SORT_DIRECTION)
        return RestChannelConsumer { channel ->
            if (transformID == null || transformID.isEmpty()) {
                val req = GetTransformsRequest(
                    searchString,
                    from,
                    size,
                    sortField,
                    sortDirection
                )
                client.execute(GetTransformsAction.INSTANCE, req, RestToXContentListener(channel))
            } else {
                val req = GetTransformRequest(transformID, if (request.method() == HEAD) FetchSourceContext.DO_NOT_FETCH_SOURCE else null)
                client.execute(GetTransformAction.INSTANCE, req, RestToXContentListener(channel))
            }
        }
    }
}
