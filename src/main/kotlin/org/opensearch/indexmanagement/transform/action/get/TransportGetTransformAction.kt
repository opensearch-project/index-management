/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.transform.action.get

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.OpenSearchStatusException
import org.opensearch.ExceptionsHelper
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.rest.RestStatus
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportGetTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetTransformRequest, GetTransformResponse> (
    GetTransformAction.NAME, transportService, actionFilters, ::GetTransformRequest
) {

    override fun doExecute(task: Task, request: GetTransformRequest, listener: ActionListener<GetTransformResponse>) {
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id)
            .fetchSourceContext(request.srcContext).preference(request.preference)
        client.get(getRequest, object : ActionListener<GetResponse> {
            override fun onResponse(response: GetResponse) {
                if (!response.isExists) {
                    listener.onFailure(OpenSearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                }

                if (response.isSourceEmpty && getRequest.fetchSourceContext() != FetchSourceContext.DO_NOT_FETCH_SOURCE) {
                    listener.onFailure(OpenSearchStatusException("Missing transform data", RestStatus.INTERNAL_SERVER_ERROR))
                } else if (response.isSourceEmpty) {
                    // For HEAD requests only
                    listener.onResponse(GetTransformResponse(
                        response.id,
                        response.version,
                        response.seqNo,
                        response.primaryTerm,
                        RestStatus.OK,
                        null))
                }

                try {
                    val contentParser = XContentHelper.createParser(xContentRegistry,
                        LoggingDeprecationHandler.INSTANCE, response.sourceAsBytesRef, XContentType.JSON)
                    val transform = contentParser.parseWithType(response.id, response.seqNo,
                        response.primaryTerm, Transform.Companion::parse)
                    listener.onResponse(GetTransformResponse(response.id, response.version, response.seqNo,
                        response.primaryTerm, RestStatus.OK, transform))
                } catch (e: Exception) {
                    listener.onFailure(
                        OpenSearchStatusException("Failed to parse transform", RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e))
                    )
                }
            }

            override fun onFailure(e: Exception) {
                    listener.onFailure(e)
            }
        })
    }
}
