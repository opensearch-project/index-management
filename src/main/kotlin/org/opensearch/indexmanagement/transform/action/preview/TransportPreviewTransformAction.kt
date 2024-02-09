/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.preview

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
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
import org.opensearch.common.settings.Settings
import org.opensearch.commons.ConfigConstants
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.opensearchapi.IndexManagementSecurityContext
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.opensearchapi.withClosableContext
import org.opensearch.indexmanagement.transform.TargetIndexMappingService
import org.opensearch.indexmanagement.transform.TransformSearchService
import org.opensearch.indexmanagement.transform.TransformValidator
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportPreviewTransformAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    val settings: Settings,
    private val client: Client,
    private val clusterService: ClusterService,
    private val indexNameExpressionResolver: IndexNameExpressionResolver,
) : HandledTransportAction<PreviewTransformRequest, PreviewTransformResponse>(
    PreviewTransformAction.NAME, transportService, actionFilters, ::PreviewTransformRequest,
) {

    private val log = LogManager.getLogger(javaClass)

    @Suppress("SpreadOperator")
    override fun doExecute(task: Task, request: PreviewTransformRequest, listener: ActionListener<PreviewTransformResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
            )}",
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
                    val user = SecurityUtils.buildUser(client.threadPool().threadContext)

                    CoroutineScope(Dispatchers.IO).launch {
                        withClosableContext(
                            IndexManagementSecurityContext("PreviewTransformHandler", settings, client.threadPool().threadContext, user),
                        ) {
                            executeSearch(searchRequest, transform, listener)
                        }
                    }
                }

                override fun onFailure(e: Exception) {
                    listener.onFailure(e)
                }
            },
        )
    }

    fun validateMappings(indices: List<String>, response: GetMappingsResponse, transform: Transform): List<String> {
        val issues = mutableListOf<String>()
        indices.forEach { index ->
            issues.addAll(TransformValidator.validateMappingsResponse(index, response, transform))
        }

        return issues
    }
    suspend fun executeSearch(searchRequest: SearchRequest, transform: Transform, listener: ActionListener<PreviewTransformResponse>) {
        val response = try {
            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            searchResponse
        } catch (e: Exception) {
            listener.onFailure(e)
            return
        }

        try {
            val targetIndexDateFieldMappings = TargetIndexMappingService.getTargetMappingsForDates(transform)
            val transformSearchResult = TransformSearchService.convertResponse(
                transform = transform, searchResponse = response, waterMarkDocuments = false,
                targetIndexDateFieldMappings = targetIndexDateFieldMappings,
            )
            val formattedResult = transformSearchResult.docsToIndex.map {
                it.sourceAsMap()
            }
            listener.onResponse(PreviewTransformResponse(formattedResult, RestStatus.OK))
        } catch (e: Exception) {
            listener.onFailure(
                OpenSearchStatusException(
                    "Failed to parse the transformed results", RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.unwrapCause(e),
                ),
            )
        }
    }
}
