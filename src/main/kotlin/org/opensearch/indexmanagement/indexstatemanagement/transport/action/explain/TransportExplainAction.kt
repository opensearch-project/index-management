/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
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
import org.opensearch.commons.ConfigConstants
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
import org.opensearch.indexmanagement.indexstatemanagement.util.MetadataCheck
import org.opensearch.indexmanagement.indexstatemanagement.util.checkMetadata
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
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
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
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

            // cluster state response will not resist the sort order
            // so use the order from previous search result saved in indexNames
            for (indexName in indexNames) {
                var metadataMapFromManagedIndex = managedIndicesMetaDataMap[indexName]
                indexPolicyIDs.add(metadataMapFromManagedIndex?.get("policy_id")) // use policyID from metadata

                val clusterStateMetadata = clusterStateIndexMetadatas[indexName]?.getManagedIndexMetadata()
                var managedIndexMetadata: ManagedIndexMetaData? = null
                val managedIndexMetadataDocUUID = indices[indexName]?.let { managedIndexMetadataID(it) }
                val configIndexMetadataMap = metadataMap[managedIndexMetadataDocUUID]
                if (metadataMapFromManagedIndex != null) {
                    if (configIndexMetadataMap != null) { // if has metadata saved, use that
                        metadataMapFromManagedIndex = configIndexMetadataMap
                    }
                    if (metadataMapFromManagedIndex.isNotEmpty()) {
                        managedIndexMetadata = ManagedIndexMetaData.fromMap(metadataMapFromManagedIndex)
                    }

//                    if (!isMetadataMoved(clusterStateMetadata, configIndexMetadataMap, log)) {
//                        val info = mapOf("message" to "Metadata is pending migration")
//                        managedIndexMetadata = clusterStateMetadata?.copy(info = info)
//                    }
                    val currentIndexUuid = indices[indexName]
                    val metadataCheck = checkMetadata(clusterStateMetadata, configIndexMetadataMap, currentIndexUuid, log)
                    if (metadataCheck == MetadataCheck.PENDING) {
                        val info = mapOf("message" to METADATA_MOVING_WARNING)
                        managedIndexMetadata = clusterStateMetadata?.copy(info = info)
                    }
                    if (metadataCheck == MetadataCheck.CORRUPT) {
                        val info = mapOf("message" to METADATA_CORRUPT_WARNING)
                        managedIndexMetadata = clusterStateMetadata?.copy(info = info)
                    }
                }
                indexMetadatas.add(managedIndexMetadata)
            }
            managedIndicesMetaDataMap.clear()

            if (user == null || indexNames.isEmpty()) {
                sendResponse()
            } else {
                filterAndSendResponse(threadContext)
            }
        }

        private fun filterAndSendResponse(threadContext: ThreadContext.StoredContext) {
            threadContext.restore()
            val filteredIndices = mutableListOf<String>()
            val filteredMetadata = mutableListOf<ManagedIndexMetaData?>()
            val filteredPolicies = mutableListOf<String?>()
            val enabledStatus = mutableMapOf<String, Boolean>()
            filter(0, filteredIndices, filteredMetadata, filteredPolicies, enabledStatus)
        }

        private fun filter(
            current: Int,
            filteredIndices: MutableList<String>,
            filteredMetadata: MutableList<ManagedIndexMetaData?>,
            filteredPolicies: MutableList<String?>,
            enabledStatus: MutableMap<String, Boolean>
        ) {
            val request = ManagedIndexRequest().indices(indexNames[current])
            client.execute(
                ManagedIndexAction.INSTANCE,
                request,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                        filteredIndices.add(indexNames[current])
                        filteredMetadata.add(indexMetadatas[current])
                        filteredPolicies.add(indexPolicyIDs[current])
                        enabledStatus[indexNames[current]] = enabledState.getOrDefault(indexNames[current], false)
                        if (current < indexNames.count() - 1) {
                            // do nothing - skip the index and go to next one
                            filter(current + 1, filteredIndices, filteredMetadata, filteredPolicies, enabledStatus)
                        } else {
                            sendResponse(filteredIndices, filteredMetadata, filteredPolicies, enabledStatus)
                        }
                    }

                    override fun onFailure(e: Exception) {
                        when (e is OpenSearchSecurityException) {
                            true -> {
                                totalManagedIndices -= 1
                                if (current < indexNames.count() - 1) {
                                    // do nothing - skip the index and go to next one
                                    filter(current + 1, filteredIndices, filteredMetadata, filteredPolicies, enabledStatus)
                                } else {
                                    sendResponse(filteredIndices, filteredMetadata, filteredPolicies, enabledStatus)
                                }
                            }
                            false -> {
                                actionListener.onFailure(e)
                            }
                        }
                    }
                }
            )
        }

        private fun sendResponse(
            indices: List<String> = indexNames,
            metadata: List<ManagedIndexMetaData?> = indexMetadatas,
            policies: List<String?> = indexPolicyIDs,
            enabledStatus: Map<String, Boolean> = enabledState,
            totalIndices: Int = totalManagedIndices
        ) {
            actionListener.onResponse(ExplainResponse(indices, policies, metadata, totalIndices, enabledStatus))
        }

        @Suppress("ReturnCount")
        private fun getMetadata(response: GetResponse?): ManagedIndexMetaData? {
            if (response == null || response.sourceAsBytesRef == null)
                return null

            try {
                val xcp = XContentHelper.createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE,
                    response.sourceAsBytesRef,
                    XContentType.JSON
                )
                return ManagedIndexMetaData.parseWithType(xcp, response.id, response.seqNo, response.primaryTerm)
            } catch (e: Exception) {
                log.error("Failed to parse the ManagedIndexMetadata for ${response.id}", e)
            }

            return null
        }
    }
    companion object {
        private const val METADATA_MOVING_WARNING = "Managed index's metadata is pending migration."
        private const val METADATA_CORRUPT_WARNING = "Managed index's metadata is corrupt, please use remove policy API to clean it."
    }
}
