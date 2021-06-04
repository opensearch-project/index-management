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

package org.opensearch.indexmanagement.transform.action.preview

import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.indexmanagement.transform.TransformSearchService
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportPreviewTransformAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    private val esClient: Client
) : HandledTransportAction<PreviewTransformRequest, PreviewTransformResponse>(
    PreviewTransformAction.NAME, transportService, actionFilters, ::PreviewTransformRequest
) {

    override fun doExecute(task: Task, request: PreviewTransformRequest, listener: ActionListener<PreviewTransformResponse>) {
        val transform = request.transform
        val searchRequest = TransformSearchService.getSearchServiceRequest(transform = transform, pageSize = 10)
        esClient.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    try {
                        val transformSearchResult = TransformSearchService.convertResponse(
                            transform = transform, searchResponse = response, waterMarkDocuments = false
                        )
                        val formattedResult = transformSearchResult.docsToIndex.map {
                            it.sourceAsMap()
                        }
                        listener.onResponse(PreviewTransformResponse(formattedResult, RestStatus.OK))
                    } catch (e: Exception) {
                        listener.onFailure(
                            OpenSearchStatusException(
                                "Failed to parse the transformed results", RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e)
                            )
                        )
                    }
                }

                override fun onFailure(e: Exception) = listener.onFailure(e)
            }
        )
    }
}
