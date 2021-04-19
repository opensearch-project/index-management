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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
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

// TODO: Field and mappings validations of source and target index, i.e. reject a histogram agg on example_field if its not possible
class TransportIndexRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val indexManagementIndices: IndexManagementIndices,
    val clusterService: ClusterService
) : HandledTransportAction<IndexRollupRequest, IndexRollupResponse>(
    IndexRollupAction.NAME, transportService, actionFilters, ::IndexRollupRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: IndexRollupRequest, listener: ActionListener<IndexRollupResponse>) {
        IndexRollupHandler(client, listener, request).start()
    }

    inner class IndexRollupHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexRollupResponse>,
        private val request: IndexRollupRequest
    ) {

        fun start() {
            indexManagementIndices.checkAndUpdateIMConfigIndex(ActionListener.wrap(::onCreateMappingsResponse, actionListener::onFailure))
        }

        private fun onCreateMappingsResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
                if (request.opType() == DocWriteRequest.OpType.CREATE) {
                    putRollup()
                } else {
                    getRollup()
                }
            } else {
                val message = "Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping."
                log.error(message)
                actionListener.onFailure(OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }

        private fun getRollup() {
            val getReq = GetRollupRequest(request.rollup.id, null)
            client.execute(GetRollupAction.INSTANCE, getReq, ActionListener.wrap(::onGetRollup, actionListener::onFailure))
        }

        @Suppress("ReturnCount")
        private fun onGetRollup(response: GetRollupResponse) {
            if (response.status != RestStatus.OK) {
                return actionListener.onFailure(OpenSearchStatusException("Unable to get existing rollup", response.status))
            }
            val rollup = response.rollup
                ?: return actionListener.onFailure(OpenSearchStatusException("The current rollup is null", RestStatus.INTERNAL_SERVER_ERROR))
            val modified = modifiedImmutableProperties(rollup, request.rollup)
            if (modified.isNotEmpty()) {
                return actionListener.onFailure(OpenSearchStatusException("Not allowed to modify $modified", RestStatus.BAD_REQUEST))
            }
            putRollup()
        }

        private fun modifiedImmutableProperties(rollup: Rollup, newRollup: Rollup): List<String> {
            val modified = mutableListOf<String>()
            if (rollup.continuous != newRollup.continuous) modified.add(Rollup.CONTINUOUS_FIELD)
            if (rollup.dimensions != newRollup.dimensions) modified.add(Rollup.DIMENSIONS_FIELD)
            if (rollup.metrics != newRollup.metrics) modified.add(Rollup.METRICS_FIELD)
            if (rollup.sourceIndex != newRollup.sourceIndex) modified.add(Rollup.SOURCE_INDEX_FIELD)
            if (rollup.targetIndex != newRollup.targetIndex) modified.add(Rollup.TARGET_INDEX_FIELD)
            if (rollup.roles != newRollup.roles) modified.add(Rollup.ROLES_FIELD)
            return modified.toList()
        }

        private fun putRollup() {
            val rollup = request.rollup.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion)
            request.index(INDEX_MANAGEMENT_INDEX)
                .id(request.rollup.id)
                .source(rollup.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .timeout(IndexRequest.DEFAULT_TIMEOUT)
            client.index(request, object : ActionListener<IndexResponse> {
                override fun onResponse(response: IndexResponse) {
                    if (response.shardInfo.failed > 0) {
                        val failureReasons = response.shardInfo.failures.joinToString(", ") { it.reason() }
                        actionListener.onFailure(OpenSearchStatusException(failureReasons, response.status()))
                    } else {
                        val status = if (request.opType() == DocWriteRequest.OpType.CREATE) RestStatus.CREATED else RestStatus.OK
                        actionListener.onResponse(
                            IndexRollupResponse(response.id, response.version, response.seqNo, response.primaryTerm, status,
                                rollup.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm))
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
