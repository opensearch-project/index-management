/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.index

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.admin.indices.mapping.get.GetMappingsAction
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.transform.TransformValidator
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.validateUserConfiguration
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

@Suppress("SpreadOperator", "LongParameterList")
class TransportIndexTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val indexManagementIndices: IndexManagementIndices,
    val indexNameExpressionResolver: IndexNameExpressionResolver,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<IndexTransformRequest, IndexTransformResponse>(
    IndexTransformAction.NAME, transportService, actionFilters, ::IndexTransformRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexTransformRequest, listener: ActionListener<IndexTransformResponse>) {
        IndexTransformHandler(client, listener, request).start()
    }

    inner class IndexTransformHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexTransformResponse>,
        private val request: IndexTransformRequest,
        private val user: User? = buildUser(client.threadPool().threadContext, request.transform.user)
    ) {

        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            client.threadPool().threadContext.stashContext().use {
                if (!validateUserConfiguration(user, filterByEnabled, actionListener)) {
                    return
                }
                indexManagementIndices.checkAndUpdateIMConfigIndex(
                    ActionListener.wrap(::onConfigIndexAcknowledgedResponse, actionListener::onFailure)
                )
            }
        }

        private fun onConfigIndexAcknowledgedResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
                if (request.opType() == DocWriteRequest.OpType.CREATE) {
                    validateAndPutTransform()
                } else {
                    updateTransform()
                }
            } else {
                val message = "Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mappings."
                log.error(message)
                actionListener.onFailure(OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun updateTransform() {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.transform.id)
            client.get(getRequest, ActionListener.wrap(::onGetTransform, actionListener::onFailure))
        }

        @Suppress("ReturnCount")
        private fun onGetTransform(response: GetResponse) {
            if (!response.isExists) {
                actionListener.onFailure(OpenSearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                return
            }

            val transform: Transform?
            try {
                transform = parseFromGetResponse(response, xContentRegistry, Transform.Companion::parse)
            } catch (e: IllegalArgumentException) {
                actionListener.onFailure(OpenSearchStatusException("Transform not found", RestStatus.NOT_FOUND))
                return
            }
            if (!userHasPermissionForResource(user, transform.user, filterByEnabled, "transform", transform.id, actionListener)) {
                return
            }
            val modified = modifiedImmutableProperties(transform, request.transform)
            if (modified.isNotEmpty()) {
                return actionListener.onFailure(OpenSearchStatusException("Not allowed to modify $modified", RestStatus.BAD_REQUEST))
            }
            putTransform()
        }

        private fun modifiedImmutableProperties(transform: Transform, newTransform: Transform): List<String> {
            val modified = mutableListOf<String>()
            if (transform.sourceIndex != newTransform.sourceIndex) modified.add(Transform.SOURCE_INDEX_FIELD)
            if (transform.targetIndex != newTransform.targetIndex) modified.add(Transform.TARGET_INDEX_FIELD)
            if (transform.dataSelectionQuery != newTransform.dataSelectionQuery) modified.add(Transform.DATA_SELECTION_QUERY_FIELD)
            if (transform.groups != newTransform.groups) modified.add(Transform.GROUPS_FIELD)
            if (transform.aggregations != newTransform.aggregations) modified.add(Transform.AGGREGATIONS_FIELD)
            if (transform.roles != newTransform.roles) modified.add(Transform.ROLES_FIELD)
            if (transform.continuous != newTransform.continuous) modified.add(Transform.CONTINUOUS_FIELD)
            return modified.toList()
        }

        private fun putTransform() {
            val transform = request.transform.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion, user = this.user)
            request.index(INDEX_MANAGEMENT_INDEX)
                .id(request.transform.id)
                .source(transform.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .timeout(IndexRequest.DEFAULT_TIMEOUT)
            client.index(
                request,
                object : ActionListener<IndexResponse> {
                    override fun onResponse(response: IndexResponse) {
                        if (response.shardInfo.failed > 0) {
                            val failureReasons = response.shardInfo.failures.joinToString(",") { it.reason() }
                            actionListener.onFailure(OpenSearchStatusException(failureReasons, response.status()))
                        } else {
                            val status = if (request.opType() == DocWriteRequest.OpType.CREATE) RestStatus.CREATED else RestStatus.OK
                            actionListener.onResponse(
                                IndexTransformResponse(
                                    response.id, response.version, response.seqNo, response.primaryTerm, status,
                                    transform.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm)
                                )
                            )
                        }
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(e)
                    }
                }
            )
        }

        private fun validateAndPutTransform() {
            val concreteIndices =
                indexNameExpressionResolver.concreteIndexNames(
                    clusterService.state(), IndicesOptions.lenientExpand(), true,
                    request.transform
                        .sourceIndex
                )
            if (concreteIndices.isEmpty()) {
                actionListener.onFailure(OpenSearchStatusException("No specified source index exist in the cluster", RestStatus.NOT_FOUND))
                return
            }

            val mappingRequest = GetMappingsRequest().indices(*concreteIndices)
            client.execute(
                GetMappingsAction.INSTANCE, mappingRequest,
                object : ActionListener<GetMappingsResponse> {
                    override fun onResponse(response: GetMappingsResponse) {
                        val issues = validateMappings(concreteIndices.toList(), response, request.transform)
                        if (issues.isNotEmpty()) {
                            val errorMessage = issues.joinToString(" ")
                            actionListener.onFailure(OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST))
                            return
                        }

                        putTransform()
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(e)
                    }
                }
            )
        }

        private fun validateMappings(indices: List<String>, response: GetMappingsResponse, transform: Transform): List<String> {
            val issues = mutableListOf<String>()
            indices.forEach { index ->
                issues.addAll(TransformValidator.validateMappingsResponse(index, response, transform))
            }

            return issues
        }
    }
}
