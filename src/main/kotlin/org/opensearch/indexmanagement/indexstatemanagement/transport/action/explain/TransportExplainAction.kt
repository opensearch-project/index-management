/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
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
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator.Companion.MAX_HITS
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.common.model.rest.SearchParams
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner.actionValidation
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getManagedIndexMetadata
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.MANAGED_INDEX_INDEX_UUID_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.util.MANAGED_INDEX_NAME_KEYWORD_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.util.MetadataCheck
import org.opensearch.indexmanagement.indexstatemanagement.util.checkMetadata
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ISMIndexMetadata
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ValidationResult
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.search.SearchHit
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportExplainAction::class.java)

// TODO: Move these to higher level and refactor plugin to make it more readable
typealias IndexUUID = String
typealias PolicyID = String
typealias IndexName = String
typealias ManagedIndexConfigDocUUID = String
typealias ManagedIndexMetadataDocUUID = String // managedIndexMetadataID(indexUuid) -> <indexUuid>#metadata
typealias ManagedIndexMetadataMap = Map<String, String?>

@Suppress("SpreadOperator", "TooManyFunctions")
class TransportExplainAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    val indexMetadataProvider: IndexMetadataProvider
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
    @Suppress("LongMethod")
    inner class ExplainHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ExplainResponse>,
        private val request: ExplainRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {
        private val indices: List<String> = request.indices
        private val explainAll: Boolean = indices.isEmpty()
        private val showPolicy: Boolean = request.showPolicy
        private val validateAction: Boolean = request.validateAction

        // Map of indexName to index metadata got from config index job which is fake/not a real full metadata document
        private val managedIndicesMetaDataMap: MutableMap<IndexName, ManagedIndexMetadataMap> = mutableMapOf()
        private val managedIndices: MutableList<IndexName> = mutableListOf()

        // indexNames are the ones that will be iterated on and shown in the final response
        // throughout request they are cleared and rewritten
        private val indexNames: MutableList<IndexName> = mutableListOf()
        private val indexNamesToUUIDs: MutableMap<IndexName, IndexUUID> = mutableMapOf()
        private val enabledState: MutableMap<IndexName, Boolean> = mutableMapOf()
        private val indexPolicyIDs = mutableListOf<PolicyID?>()
        private val indexMetadatas = mutableListOf<ManagedIndexMetaData?>()
        private val validationResults = mutableListOf<ValidationResult?>()
        private var totalManagedIndices = 0
        private val appliedPolicies: MutableMap<String, Policy> = mutableMapOf()
        private val policiesforValidation: MutableMap<String, Policy> = mutableMapOf()

        @Suppress("SpreadOperator", "NestedBlockDepth")
        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
                )}"
            )
            // Use the indexMetadataProvider to get the index names and uuids corresponding to this index type
            CoroutineScope(Dispatchers.IO).launch {
                val indexNameToMetadata: MutableMap<String, ISMIndexMetadata> = HashMap()
                try {
                    if (explainAll) {
                        indexNameToMetadata.putAll(indexMetadataProvider.getAllISMIndexMetadataByType(request.indexType))
                    } else {
                        indexNameToMetadata.putAll(indexMetadataProvider.getISMIndexMetadataByType(request.indexType, indices))
                    }
                } catch (e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    return@launch
                }
                // These index names are resolved and populated by the indexMetadataProvider specific to the index type
                indexNames.addAll(indexNameToMetadata.keys)
                indexNamesToUUIDs.putAll(indexNameToMetadata.mapValues { it.value.indexUuid })

                val params = request.searchParams
                val searchRequest = getSearchMetadataRequest(params, indexNamesToUUIDs.values.toList(), if (explainAll) params.size else MAX_HITS)
                searchForMetadata(searchRequest)
            }
        }

        private fun getSearchMetadataRequest(params: SearchParams, indexUUIDs: List<String>, searchSize: Int): SearchRequest {
            val sortBuilder = params.getSortBuilder()

            val queryBuilder = QueryBuilders.boolQuery()
                .must(
                    QueryBuilders
                        .queryStringQuery(params.queryString)
                        .defaultField(MANAGED_INDEX_NAME_KEYWORD_FIELD)
                        .defaultOperator(Operator.AND)
                ).filter(QueryBuilders.termsQuery(MANAGED_INDEX_INDEX_UUID_FIELD, indexUUIDs))

            val searchSourceBuilder = SearchSourceBuilder()
                .from(params.from)
                .fetchSource(FETCH_SOURCE)
                .seqNoAndPrimaryTerm(true)
                .version(true)
                .sort(sortBuilder)
                .size(searchSize)
                .query(queryBuilder)

            return SearchRequest()
                .indices(INDEX_MANAGEMENT_INDEX)
                .source(searchSourceBuilder)
        }

        private fun searchForMetadata(searchRequest: SearchRequest) {
            client.threadPool().threadContext.stashContext().use { threadContext ->
                client.search(
                    searchRequest,
                    object : ActionListener<SearchResponse> {
                        override fun onResponse(response: SearchResponse) {
                            val totalHits = response.hits.totalHits
                            if (totalHits != null) {
                                totalManagedIndices = totalHits.value.toInt()
                            }

                            parseSearchHits(response.hits.hits).forEach { managedIndex ->
                                managedIndices.add(managedIndex.index)
                                enabledState[managedIndex.index] = managedIndex.enabled
                                managedIndicesMetaDataMap[managedIndex.index] = mapOf(
                                    "index" to managedIndex.index,
                                    "index_uuid" to managedIndex.indexUuid,
                                    "policy_id" to managedIndex.policyID,
                                    "enabled" to managedIndex.enabled.toString()
                                )
                                if (showPolicy) {
                                    managedIndex.policy?.let { appliedPolicies[managedIndex.index] = it }
                                }
                                if (validateAction) {
                                    managedIndex.policy?.let { policiesforValidation[managedIndex.index] = it }
                                }
                            }

                            // explain all only return managed indices
                            if (explainAll) {
                                if (managedIndices.size == 0) {
                                    // edge case: if specify query param pagination size to be 0
                                    // we still show total managed indices
                                    indexNames.clear()
                                    sendResponse(
                                        indexNames, indexMetadatas, indexPolicyIDs, enabledState,
                                        totalManagedIndices, appliedPolicies, validationResults
                                    )
                                    return
                                } else {
                                    // Clear and add the managedIndices from the response to preserve the sort order and size
                                    indexNames.clear()
                                    indexNames.addAll(managedIndices)
                                    // Remove entries in case they were limited due to request size
                                    indexNamesToUUIDs.filterKeys { indexNames.contains(it) }
                                    getMetadata(indexNames, threadContext)
                                    return
                                }
                            }

                            // explain/{index} return results for all indices
                            getMetadata(indexNames, threadContext)
                        }

                        override fun onFailure(t: Exception) {
                            if (t is IndexNotFoundException) {
                                // config index hasn't been initialized
                                // show all requested indices not managed
                                if (!explainAll) {
                                    getMetadata(indexNames, threadContext)
                                    return
                                }
                                indexNames.clear()
                                sendResponse(
                                    indexNames, indexMetadatas, indexPolicyIDs,
                                    enabledState, totalManagedIndices, appliedPolicies, validationResults
                                )
                                return
                            }
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                        }
                    }
                )
            }
        }

        @Suppress("SpreadOperator")
        fun getMetadata(indexNames: List<String>, threadContext: ThreadContext.StoredContext) {
            if (request.indexType == DEFAULT_INDEX_TYPE) {
                val clusterStateRequest = ClusterStateRequest()
                clusterStateRequest.clear()
                    .indices(*indexNames.toTypedArray())
                    .metadata(true)
                    .local(request.local)
                    .clusterManagerNodeTimeout(request.clusterManagerTimeout)

                client.admin().cluster().state(
                    clusterStateRequest,
                    object : ActionListener<ClusterStateResponse> {
                        override fun onResponse(response: ClusterStateResponse) {
                            val clusterStateIndexMetadatas = response.state.metadata.indices.associate { it.key to it.value }
                            getMetadataMap(clusterStateIndexMetadatas, threadContext)
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                        }
                    }
                )
            } else {
                getMetadataMap(null, threadContext)
            }
        }

        private fun getMetadataMap(clusterStateIndexMetadatas: Map<IndexName, IndexMetadata>?, threadContext: ThreadContext.StoredContext) {
            val mgetMetadataReq = MultiGetRequest()
            indexNamesToUUIDs.values.forEach { uuid ->
                mgetMetadataReq.add(MultiGetRequest.Item(INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(uuid)).routing(uuid))
            }
            client.multiGet(
                mgetMetadataReq,
                object : ActionListener<MultiGetResponse> {
                    override fun onResponse(response: MultiGetResponse) {
                        val metadataMap: Map<ManagedIndexMetadataDocUUID, ManagedIndexMetadataMap?> =
                            response.responses.associate { it.id to getMetadata(it.response)?.toMap() }
                        buildResponse(indexNamesToUUIDs, metadataMap, clusterStateIndexMetadatas, threadContext)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }

        @Suppress("ComplexMethod", "NestedBlockDepth")
        private fun buildResponse(
            indices: Map<IndexName, IndexUUID>,
            metadataMap: Map<ManagedIndexMetadataDocUUID, ManagedIndexMetadataMap?>,
            clusterStateIndexMetadatas: Map<IndexName, IndexMetadata>?,
            threadContext: ThreadContext.StoredContext
        ) {
            // cluster state response will not resist the sort order
            // so use the order from previous search result saved in indexNames
            for (indexName in indexNames) {
                var metadataMapFromManagedIndex = managedIndicesMetaDataMap[indexName]
                indexPolicyIDs.add(metadataMapFromManagedIndex?.get("policy_id"))

                var managedIndexMetadata: ManagedIndexMetaData? = null
                val managedIndexMetadataDocUUID = indices[indexName]?.let { managedIndexMetadataID(it) }
                val configIndexMetadataMap = metadataMap[managedIndexMetadataDocUUID]
                if (metadataMapFromManagedIndex != null) {
                    if (configIndexMetadataMap != null) {
                        metadataMapFromManagedIndex = configIndexMetadataMap
                    }
                    if (metadataMapFromManagedIndex.isNotEmpty()) {
                        managedIndexMetadata = ManagedIndexMetaData.fromMap(metadataMapFromManagedIndex)
                    }

                    // clusterStateIndexMetadatas will not be null only for the default index type
                    if (clusterStateIndexMetadatas != null) {
                        val currentIndexUuid = indices[indexName]
                        val clusterStateMetadata = clusterStateIndexMetadatas[indexName]?.getManagedIndexMetadata()
                        val metadataCheck = checkMetadata(clusterStateMetadata, configIndexMetadataMap, currentIndexUuid, log)
                        val info = metadataStatusToInfo[metadataCheck]
                        info?.let { managedIndexMetadata = clusterStateMetadata?.copy(info = it) }
                    }
                }
                if (validateAction) {
                    var validationResult = actionValidation.validate("nothing", indexName)
                    val policy = policiesforValidation[indexName]
                    if (policy != null && managedIndexMetadata != null) {
                        val state = policy.getStateToExecute(managedIndexMetadata!!)
                        val action = state?.getActionToExecute(managedIndexMetadata!!, indexMetadataProvider)
                        var actionName = action?.type
                        if (actionName == null) {
                            actionName = "nothing"
                        }
                        validationResult = actionValidation.validate(actionName, indexName)
                    }
                    validationResults.add(validationResult)
                } else {
                    validationResults.add(null)
                }

                indexMetadatas.add(managedIndexMetadata)
            }
            managedIndicesMetaDataMap.clear()

            if (user == null || indexNames.isEmpty()) {
                sendResponse(indexNames, indexMetadatas, indexPolicyIDs, enabledState, totalManagedIndices, appliedPolicies, validationResults)
            } else {
                filterAndSendResponse(threadContext)
            }
        }

        private fun filterAndSendResponse(threadContext: ThreadContext.StoredContext) {
            threadContext.restore()
            val filteredIndices = mutableListOf<String>()
            val filteredMetadata = mutableListOf<ManagedIndexMetaData?>()
            val filteredValidationResult = mutableListOf<ValidationResult?>()
            val filteredPolicies = mutableListOf<PolicyID?>()
            val enabledStatus = mutableMapOf<String, Boolean>()
            val filteredAppliedPolicies = mutableMapOf<String, Policy>()

            CoroutineScope(Dispatchers.IO).launch {
                // filter out indicies for which user doesn't have manage index permissions
                for (i in 0 until indexNames.count()) {
                    val request = ManagedIndexRequest().indices(indexNames[i])
                    try {
                        client.suspendUntil<NodeClient, AcknowledgedResponse> { execute(ManagedIndexAction.INSTANCE, request, it) }
                        filteredIndices.add(indexNames[i])
                        filteredMetadata.add(indexMetadatas[i])
                        filteredPolicies.add(indexPolicyIDs[i])
                        validationResults[i]?.let { filteredValidationResult.add(it) }
                        enabledState[indexNames[i]]?.let { enabledStatus[indexNames[i]] = it }
                        appliedPolicies[indexNames[i]]?.let { filteredAppliedPolicies[indexNames[i]] = it }
                    } catch (e: OpenSearchSecurityException) {
                        totalManagedIndices -= 1
                    } catch (e: Exception) {
                        actionListener.onFailure(e)
                    }
                }
                sendResponse(
                    filteredIndices, filteredMetadata, filteredPolicies, enabledStatus,
                    totalManagedIndices, filteredAppliedPolicies, filteredValidationResult
                )
            }
        }

        @Suppress("LongParameterList")
        private fun sendResponse(
            indices: List<String>,
            metadata: List<ManagedIndexMetaData?>,
            policyIDs: List<PolicyID?>,
            enabledStatus: Map<String, Boolean>,
            totalIndices: Int,
            policies: Map<String, Policy>,
            validationResult: List<ValidationResult?>,
        ) {
            actionListener.onResponse(ExplainResponse(indices, policyIDs, metadata, totalIndices, enabledStatus, policies, validationResult))
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

        private fun parseSearchHits(hits: Array<SearchHit>): List<ManagedIndexConfig> {
            return hits.map { hit ->
                XContentHelper.createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef,
                    XContentType.JSON
                ).parseWithType(parse = ManagedIndexConfig.Companion::parse)
            }
        }
    }

    companion object {
        const val METADATA_MOVING_WARNING = "Managed index's metadata is pending migration."
        const val METADATA_CORRUPT_WARNING = "Managed index's metadata is corrupt, please use remove policy API to clean it."
        val metadataStatusToInfo = mapOf(
            MetadataCheck.PENDING to mapOf("message" to METADATA_MOVING_WARNING),
            MetadataCheck.CORRUPT to mapOf("message" to METADATA_CORRUPT_WARNING)
        )
    }
}
