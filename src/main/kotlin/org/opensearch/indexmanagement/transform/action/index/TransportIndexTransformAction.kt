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

package org.opensearch.indexmanagement.transform.action.index

import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.transform.action.get.GetTransformAction
import org.opensearch.indexmanagement.transform.action.get.GetTransformRequest
import org.opensearch.indexmanagement.transform.action.get.GetTransformResponse
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.util.IndexUtils
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportIndexTransformAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val indexManagementIndices: IndexManagementIndices,
    val clusterService: ClusterService
) : HandledTransportAction<IndexTransformRequest, IndexTransformResponse>(
        IndexTransformAction.NAME, transportService, actionFilters, ::IndexTransformRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexTransformRequest, listener: ActionListener<IndexTransformResponse>) {
        IndexTransformHandler(client, listener, request).start()
    }

    inner class IndexTransformHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexTransformResponse>,
        private val request: IndexTransformRequest
    ) {

        fun start() {
            indexManagementIndices.checkAndUpdateIMConfigIndex(ActionListener.wrap(::onConfigIndexAcknowledgedResponse, actionListener::onFailure))
        }

        private fun onConfigIndexAcknowledgedResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
                if (request.opType() == DocWriteRequest.OpType.CREATE) {
                    putTransform()
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
            val getReq = GetTransformRequest(request.transform.id, null, null)
            client.execute(GetTransformAction.INSTANCE, getReq, ActionListener.wrap(::onGetTransform, actionListener::onFailure))
        }

        @Suppress("ReturnCount")
        private fun onGetTransform(response: GetTransformResponse) {
            if (response.status != RestStatus.OK) {
                return actionListener.onFailure(OpenSearchStatusException("Unable to get existing transform", response.status))
            }
            val transform = response.transform
                ?: return actionListener.onFailure(OpenSearchStatusException("The current transform is null", RestStatus.INTERNAL_SERVER_ERROR))
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
            return modified.toList()
        }

        private fun putTransform() {
            val transform = request.transform.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion)
            request.index(INDEX_MANAGEMENT_INDEX)
                .id(request.transform.id)
                .source(transform.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .timeout(IndexRequest.DEFAULT_TIMEOUT)
            client.index(request, object : ActionListener<IndexResponse> {
                override fun onResponse(response: IndexResponse) {
                    if (response.shardInfo.failed > 0) {
                        val failureReasons = response.shardInfo.failures.joinToString(",") { it.reason() }
                        actionListener.onFailure(OpenSearchStatusException(failureReasons, response.status()))
                    } else {
                        val status = if (request.opType() == DocWriteRequest.OpType.CREATE) RestStatus.CREATED else RestStatus.OK
                        actionListener.onResponse(
                                IndexTransformResponse(response.id, response.version, response.seqNo, response.primaryTerm, status,
                                        transform.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm))
                        )
                    }
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(e)
                }
            })
        }
    }
}
