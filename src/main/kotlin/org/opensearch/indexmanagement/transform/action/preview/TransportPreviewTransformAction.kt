/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.preview

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.mapping.get.GetMappingsAction
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.IndicesOptions
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.commons.ConfigConstants
import org.opensearch.indexmanagement.transform.TransformSearchService
import org.opensearch.indexmanagement.transform.TransformValidator
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportPreviewTransformAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    private val client: Client,
    private val clusterService: ClusterService,
    private val indexNameExpressionResolver: IndexNameExpressionResolver
) : HandledTransportAction<PreviewTransformRequest, PreviewTransformResponse>(
    PreviewTransformAction.NAME, transportService, actionFilters, ::PreviewTransformRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator")
    override fun doExecute(task: Task, request: PreviewTransformRequest, listener: ActionListener<PreviewTransformResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val transform = request.transform

        val concreteIndices =
            indexNameExpressionResolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpand(), true, transform.sourceIndex)
        if (concreteIndices.isEmpty()) {
            listener.onFailure(OpenSearchStatusException("No specified source index exist in the cluster", RestStatus.NOT_FOUND))
            return
        }

        val mappingRequest = GetMappingsRequest().indices(*concreteIndices)
        client.execute(
            GetMappingsAction.INSTANCE, mappingRequest,
            object : ActionListener<GetMappingsResponse> {
                override fun onResponse(response: GetMappingsResponse) {
                    val issues = validateMappings(concreteIndices.toList(), response, transform)
                    if (issues.isNotEmpty()) {
                        val errorMessage = issues.joinToString(" ")
                        listener.onFailure(OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST))
                        return
                    }
                    val searchRequest = TransformSearchService.getSearchServiceRequest(transform = transform, pageSize = 10)
                    executeSearch(searchRequest, transform, listener)
                }

                override fun onFailure(e: Exception) {
                    listener.onFailure(e)
                }
            }
        )
    }

    fun validateMappings(indices: List<String>, response: GetMappingsResponse, transform: Transform): List<String> {
        val issues = mutableListOf<String>()
        indices.forEach { index ->
            issues.addAll(TransformValidator.validateMappingsResponse(index, response, transform))
        }

        return issues
    }

    fun executeSearch(searchRequest: SearchRequest, transform: Transform, listener: ActionListener<PreviewTransformResponse>) {
        client.search(
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
