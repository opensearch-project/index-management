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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.get.GetResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.authuser.User
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getManagedIndexMetadata
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.isMetadataMoved
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.util.IndexManagementException
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE
import org.opensearch.search.sort.SortBuilders
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportExplainAction::class.java)

@Suppress("SpreadOperator")
class TransportExplainAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ExplainRequest, ExplainResponse>(
    ExplainAction.NAME, transportService, actionFilters, ::ExplainRequest
) {

    override fun doExecute(task: Task, request: ExplainRequest, listener: ActionListener<ExplainResponse>) {
        ExplainHandler(client, listener, request).start()
    }

    /**
     * first search config index to find out managed indices
     * then retrieve metadata of these managed indices
     * special case: when user explicitly query for an un-managed index
     * return this index with its policy id shown 'null' meaning it's not managed
     */
    inner class ExplainHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ExplainResponse>,
        private val request: ExplainRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {
        private val indices: List<String> = request.indices
        private val explainAll: Boolean = indices.isEmpty()
        private val wildcard: Boolean = indices.any { it.contains("*") }

        // map of index to index metadata got from config index job
        private val managedIndicesMetaDataMap: MutableMap<String, Map<String, String?>> = mutableMapOf()
        private val managedIndices: MutableList<String> = mutableListOf()

        private val indexNames: MutableList<String> = mutableListOf()
        private val enabledState: MutableMap<String, Boolean> = mutableMapOf()
        private val indexPolicyIDs = mutableListOf<String?>()
        private val indexMetadatas = mutableListOf<ManagedIndexMetaData?>()
        private var totalManagedIndices = 0

        @Suppress("SpreadOperator", "NestedBlockDepth")
        fun start() {
            val params = request.searchParams

            val sortBuilder = SortBuilders
                .fieldSort(params.sortField)
                .order(SortOrder.fromString(params.sortOrder))

            val queryBuilder = QueryBuilders.boolQuery()
                .must(
                    QueryBuilders
                        .queryStringQuery(params.queryString)
                        .defaultField("managed_index.name")
                        .defaultOperator(Operator.AND)
                )

            var searchSourceBuilder = SearchSourceBuilder()
                .from(params.from)
                .fetchSource(FETCH_SOURCE)
                .seqNoAndPrimaryTerm(true)
                .version(true)
                .sort(sortBuilder)

            if (!explainAll) {
                searchSourceBuilder = searchSourceBuilder.size(MAX_HITS)
                if (wildcard) { // explain/index*
                    indices.forEach {
                        if (it.contains("*")) {
                            queryBuilder.should(QueryBuilders.wildcardQuery("managed_index.index", it))
                        } else {
                            queryBuilder.should(QueryBuilders.termsQuery("managed_index.index", it))
                        }
                    }
                } else { // explain/{index}
                    queryBuilder.filter(QueryBuilders.termsQuery("managed_index.index", indices))
                }
            } else { // explain all
                searchSourceBuilder = searchSourceBuilder.size(params.size)
                queryBuilder.filter(QueryBuilders.existsQuery("managed_index"))
            }

            searchSourceBuilder = searchSourceBuilder.query(queryBuilder)

            val searchRequest = SearchRequest()
                .indices(INDEX_MANAGEMENT_INDEX)
                .source(searchSourceBuilder)

            client.threadPool().threadContext.stashContext().use { threadContext ->
                client.search(
                    searchRequest,
                    object : ActionListener<SearchResponse> {
                        override fun onResponse(response: SearchResponse) {
                            val totalHits = response.hits.totalHits
                            if (totalHits != null) {
                                totalManagedIndices = totalHits.value.toInt()
                            }

                            response.hits.hits.map {
                                val hitMap = it.sourceAsMap["managed_index"] as Map<String, Any>
                                val managedIndex = hitMap["index"] as String
                                managedIndices.add(managedIndex)
                                enabledState[managedIndex] = hitMap["enabled"] as Boolean
                                managedIndicesMetaDataMap[managedIndex] = mapOf(
                                    "index" to hitMap["index"] as String?,
                                    "index_uuid" to hitMap["index_uuid"] as String?,
                                    "policy_id" to hitMap["policy_id"] as String?,
                                    "enabled" to hitMap["enabled"]?.toString()
                                )
                            }

                            // explain all only return managed indices
                            if (explainAll) {
                                if (managedIndices.size == 0) {
                                    // edge case: if specify query param pagination size to be 0
                                    // we still show total managed indices
                                    sendResponse()
                                    return
                                } else {
                                    indexNames.addAll(managedIndices)
                                    getMetadata(managedIndices, threadContext)
                                    return
                                }
                            }

                            // explain/{index} return results for all indices
                            indexNames.addAll(indices)
                            getMetadata(indices, threadContext)
                        }

                        override fun onFailure(t: Exception) {
                            if (t is IndexNotFoundException) {
                                // config index hasn't been initialized
                                // show all requested indices not managed
                                if (indices.isNotEmpty()) {
                                    indexNames.addAll(indices)
                                    getMetadata(indices, threadContext)
                                    return
                                }
                                sendResponse()
                                return
                            }
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                        }
                    }
                )
            }
        }

        @Suppress("SpreadOperator")
        fun getMetadata(indices: List<String>, threadContext: ThreadContext.StoredContext) {
            val clusterStateRequest = ClusterStateRequest()
            val strictExpandIndicesOptions = IndicesOptions.strictExpand()

            clusterStateRequest.clear()
                .indices(*indices.toTypedArray())
                .metadata(true)
                .local(request.local)
                .masterNodeTimeout(request.masterTimeout)
                .indicesOptions(strictExpandIndicesOptions)

            client.admin().cluster().state(
                clusterStateRequest,
                object : ActionListener<ClusterStateResponse> {
                    override fun onResponse(response: ClusterStateResponse) {
                        onClusterStateResponse(response, threadContext)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }

        fun onClusterStateResponse(clusterStateResponse: ClusterStateResponse, threadContext: ThreadContext.StoredContext) {
            val clusterStateIndexMetadatas = clusterStateResponse.state.metadata.indices.map { it.key to it.value }.toMap()

            if (wildcard) {
                indexNames.clear() // clear wildcard (index*) from indexNames
                clusterStateIndexMetadatas.forEach { indexNames.add(it.key) }
            }

            val indices = clusterStateIndexMetadatas.map { it.key to it.value.indexUUID }.toMap()
            val mgetMetadataReq = MultiGetRequest()
            indices.map { it.value }.forEach { uuid ->
                mgetMetadataReq.add(MultiGetRequest.Item(INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(uuid)).routing(uuid))
            }
            client.multiGet(
                mgetMetadataReq,
                object : ActionListener<MultiGetResponse> {
                    override fun onResponse(response: MultiGetResponse) {
                        val metadataMap = response.responses.map { it.id to getMetadata(it.response)?.toMap() }.toMap()
                        buildResponse(indices, metadataMap, clusterStateIndexMetadatas, threadContext)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }

        // metadataMap: doc id -> metadataMap, doc id for metadata is [managedIndexMetadataID(indexUuid)]
        fun buildResponse(
            indices: Map<String, String>,
            metadataMap: Map<String, Map<String, String>?>,
            clusterStateIndexMetadatas: Map<String, IndexMetadata>,
            threadContext: ThreadContext.StoredContext
        ) {

            // cluster state response will not resisting the sort order
            // so use the order from previous search result saved in indexNames
            for (indexName in indexNames) {
                var managedIndexMetadataMap = managedIndicesMetaDataMap[indexName]
                indexPolicyIDs.add(managedIndexMetadataMap?.get("policy_id")) // use policyID from metadata

                val clusterStateMetadata = clusterStateIndexMetadatas[indexName]?.getManagedIndexMetadata()
                var managedIndexMetadata: ManagedIndexMetaData? = null
                val configIndexMetadataMap = metadataMap[indices[indexName]?.let { managedIndexMetadataID(it) }]
                if (managedIndexMetadataMap != null) {
                    if (configIndexMetadataMap != null) { // if has metadata saved, use that
                        managedIndexMetadataMap = configIndexMetadataMap
                    }
                    if (managedIndexMetadataMap.isNotEmpty()) {
                        managedIndexMetadata = ManagedIndexMetaData.fromMap(managedIndexMetadataMap)
                    }

                    if (!isMetadataMoved(clusterStateMetadata, configIndexMetadataMap, log)) {
                        val info = mapOf("message" to "Metadata is pending migration")
                        managedIndexMetadata = clusterStateMetadata?.copy(info = info)
                    }
                }
                indexMetadatas.add(managedIndexMetadata)
            }
            managedIndicesMetaDataMap.clear()

            if (user == null) {
                sendResponse()
            } else {
                validateAndSendResponse(threadContext)
            }
        }

        private fun validateAndSendResponse(threadContext: ThreadContext.StoredContext) {
            threadContext.restore()
            val request = ManagedIndexRequest().indices(*indexNames.toTypedArray())
            client.execute(
                ManagedIndexAction.INSTANCE,
                request,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                        sendResponse()
                    }

                    override fun onFailure(e: java.lang.Exception) {
                        actionListener.onFailure(
                            IndexManagementException.wrap(
                                when (e is OpenSearchSecurityException) {
                                    true -> OpenSearchStatusException(
                                        "User doesn't have required index permissions on one or more requested indices: ${e.localizedMessage}",
                                        RestStatus.FORBIDDEN
                                    )
                                    false -> e
                                }
                            )
                        )
                    }
                }
            )
        }

        private fun sendResponse() {
            if (explainAll) {
                actionListener.onResponse(ExplainAllResponse(indexNames, indexPolicyIDs, indexMetadatas, totalManagedIndices, enabledState))
                return
            }
            actionListener.onResponse(ExplainResponse(indexNames, indexPolicyIDs, indexMetadatas))
        }

        private fun getMetadata(response: GetResponse?): ManagedIndexMetaData? {
            if (response == null || response.sourceAsBytesRef == null)
                return null

            val xcp = XContentHelper.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                response.sourceAsBytesRef,
                XContentType.JSON
            )
            return ManagedIndexMetaData.parseWithType(
                xcp,
                response.id, response.seqNo, response.primaryTerm
            )
        }
    }
}
