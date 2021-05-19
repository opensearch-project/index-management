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
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.elasticapi.getUuidsForClosedIndices
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import org.opensearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexMetadataRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexRequest
import org.opensearch.indexmanagement.util.IndexManagementException
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.IndicesOptions
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.common.inject.Inject
import org.opensearch.index.Index
import org.opensearch.index.IndexNotFoundException
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportRemovePolicyAction::class.java)

class TransportRemovePolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters
) : HandledTransportAction<RemovePolicyRequest, ISMStatusResponse>(
        RemovePolicyAction.NAME, transportService, actionFilters, ::RemovePolicyRequest
) {
    override fun doExecute(task: Task, request: RemovePolicyRequest, listener: ActionListener<ISMStatusResponse>) {
        RemovePolicyHandler(client, listener, request).start()
    }

    inner class RemovePolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: RemovePolicyRequest
    ) {

        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val indicesToRemove = mutableMapOf<String, String>() // uuid: name

        @Suppress("SpreadOperator")
        fun start() {
            val strictExpandOptions = IndicesOptions.strictExpand()

            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(*request.indices.toTypedArray())
                .metadata(true)
                .local(false)
                .indicesOptions(strictExpandOptions)

            client.admin()
                .cluster()
                .state(clusterStateRequest, object : ActionListener<ClusterStateResponse> {
                    override fun onResponse(response: ClusterStateResponse) {
                        val indexMetadatas = response.state.metadata.indices
                        indexMetadatas.forEach {
                            indicesToRemove.putIfAbsent(it.value.indexUUID, it.key)
                        }
                        populateLists(response.state)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                })
        }

        private fun populateLists(state: ClusterState) {
            getUuidsForClosedIndices(state).forEach {
                failedIndices.add(FailedIndex(indicesToRemove[it] as String, it, "This index is closed"))
                indicesToRemove.remove(it)
            }
            if (indicesToRemove.isEmpty()) {
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                return
            }

            val multiGetReq = MultiGetRequest()
            indicesToRemove.forEach { multiGetReq.add(INDEX_MANAGEMENT_INDEX, it.key) }

            client.multiGet(multiGetReq, object : ActionListener<MultiGetResponse> {
                override fun onResponse(response: MultiGetResponse) {
                    // config index may not be initialized
                    val f = response.responses.first()
                    if (f.isFailed && f.failure.failure is IndexNotFoundException) {
                        indicesToRemove.forEach { (uuid, name) ->
                            failedIndices.add(FailedIndex(name, uuid, "This index does not have a policy to remove"))
                        }
                        actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                        return
                    }

                    response.forEach {
                        if (!it.response.isExists) {
                            val docId = it.id // docId is managed index uuid
                            failedIndices.add(FailedIndex(indicesToRemove[docId] as String, docId,
                                    "This index does not have a policy to remove"))
                            indicesToRemove.remove(docId)
                        }
                    }

                    removeManagedIndices()
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                }
            })
        }

        @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
        fun removeManagedIndices() {
            if (indicesToRemove.isNotEmpty()) {
                val bulkReq = BulkRequest()
                indicesToRemove.forEach { bulkReq.add(deleteManagedIndexRequest(it.key)) }
                client.bulk(bulkReq, object : ActionListener<BulkResponse> {
                    override fun onResponse(response: BulkResponse) {
                        response.forEach {
                            val docId = it.id // docId is indexUuid of the managed index
                            if (it.isFailed) {
                                failedIndices.add(FailedIndex(indicesToRemove[docId] as String, docId, "Failed to remove policy"))
                                indicesToRemove.remove(docId)
                            }
                        }

                        // clean metadata for indicesToRemove
                        val indicesToRemoveMetadata = indicesToRemove.map { Index(it.value, it.key) }
                        removeMetadatas(indicesToRemoveMetadata)
                    }

                    override fun onFailure(t: Exception) {
                        if (t is ClusterBlockException) {
                            indicesToRemove.forEach { (uuid, name) ->
                                failedIndices.add(FailedIndex(name, uuid, "Failed to remove policy due to ClusterBlockingException: ${t.message}"))
                            }
                            actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                        } else {
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                        }
                    }
                })
            } else {
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
            }
        }

        fun removeMetadatas(indices: List<Index>) {
            val request = indices.map { deleteManagedIndexMetadataRequest(it.uuid) }
            val bulkReq = BulkRequest().add(request)
            client.bulk(bulkReq, object : ActionListener<BulkResponse> {
                override fun onResponse(response: BulkResponse) {
                    response.forEach {
                        val docId = it.id
                        if (it.isFailed) {
                            failedIndices.add(FailedIndex(indicesToRemove[docId] as String, docId,
                                "Failed to clean metadata due to: ${it.failureMessage}"))
                            indicesToRemove.remove(docId)
                        }
                    }
                    actionListener.onResponse(ISMStatusResponse(indicesToRemove.size, failedIndices))
                }

                override fun onFailure(e: Exception) {
                    actionListener.onFailure(IndexManagementException.wrap(
                        Exception("Failed to clean metadata for remove policy indices.", e)))
                }
            })
        }
    }
}
