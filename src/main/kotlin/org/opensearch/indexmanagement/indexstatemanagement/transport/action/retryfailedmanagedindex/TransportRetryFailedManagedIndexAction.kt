/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.update.UpdateRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import org.opensearch.core.index.Index
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.DefaultIndexMetadataService
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.buildMgetMetadataRequest
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.mgetResponseToMap
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.opensearch.indexmanagement.indexstatemanagement.util.isFailed
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.indexstatemanagement.util.updateEnableManagedIndexRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ISMIndexMetadata
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.util.IndexManagementException
import org.opensearch.indexmanagement.util.RunAsSubjectClient
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val log = LogManager.getLogger(TransportRetryFailedManagedIndexAction::class.java)

@Suppress("SpreadOperator")
class TransportRetryFailedManagedIndexAction
@Inject
constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val indexMetadataProvider: IndexMetadataProvider,
    val pluginClient: RunAsSubjectClient,
) : HandledTransportAction<RetryFailedManagedIndexRequest, ISMStatusResponse>(
    RetryFailedManagedIndexAction.NAME, transportService, actionFilters, ::RetryFailedManagedIndexRequest,
) {
    override fun doExecute(task: Task, request: RetryFailedManagedIndexRequest, listener: ActionListener<ISMStatusResponse>) {
        RetryFailedManagedIndexHandler(client, listener, request).start()
    }

    inner class RetryFailedManagedIndexHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<ISMStatusResponse>,
        private val request: RetryFailedManagedIndexRequest,
        private val user: User? = buildUser(client.threadPool().threadContext),
    ) {
        private val failedIndices: MutableList<FailedIndex> = mutableListOf()
        private val listOfMetadata: MutableList<ManagedIndexMetaData> = mutableListOf()
        private val listOfIndexToMetadata: MutableList<Pair<Index, ManagedIndexMetaData>> = mutableListOf()
        private val mapOfItemIdToIndex: MutableMap<Int, Index> = mutableMapOf()
        private val indicesManagedState: MutableMap<String, Boolean> = mutableMapOf()
        private var indicesToRetry = mutableMapOf<String, String>() // uuid: name
        private val indexUuidToIndexMetadata = mutableMapOf<String, IndexMetadata>() // uuid -> indexmetadata

        @Suppress("SpreadOperator")
        fun start() {
            log.debug(
                "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                    ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
                )}",
            )
            if (user == null) {
                // Security plugin is not enabled
                getIndicesToRetry()
            } else {
                validateAndGetIndicesToRetry()
            }
        }

        private fun validateAndGetIndicesToRetry() {
            val managedIndexRequest = ManagedIndexRequest().indices(*request.indices.toTypedArray())
            client.execute(
                ManagedIndexAction.INSTANCE,
                managedIndexRequest,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(response: AcknowledgedResponse) {
                        getIndicesToRetry()
                    }

                    override fun onFailure(e: java.lang.Exception) {
                        actionListener.onFailure(
                            IndexManagementException.wrap(
                                when (e is OpenSearchSecurityException) {
                                    true ->
                                        OpenSearchStatusException(
                                            "User doesn't have required index permissions on one or more requested indices: ${e.localizedMessage}",
                                            RestStatus.FORBIDDEN,
                                        )
                                    false -> e
                                },
                            ),
                        )
                    }
                },
            )
        }

        private fun getIndicesToRetry() {
            CoroutineScope(Dispatchers.IO).launch {
                val indexNameToMetadata: MutableMap<String, ISMIndexMetadata> = HashMap()
                try {
                    indexNameToMetadata.putAll(indexMetadataProvider.getISMIndexMetadataByType(request.indexType, request.indices))
                } catch (e: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    return@launch
                }
                indexNameToMetadata.forEach { (indexName, indexMetadata) ->
                    indicesToRetry.putIfAbsent(indexMetadata.indexUuid, indexName)
                }
                if (request.indexType == DEFAULT_INDEX_TYPE) {
                    getClusterState()
                } else {
                    processResponse()
                }
            }
        }

        private fun getClusterState() {
            val clusterService = indexMetadataProvider.clusterService
            val currentState = clusterService.state()

            val indicesNames = request.indices.toTypedArray()

            try {
                val defaultIndexMetadataService = indexMetadataProvider.services[DEFAULT_INDEX_TYPE] as DefaultIndexMetadataService

                // Get indices metadata from the cluster state
                currentState.metadata.indices
                    .filter { (name, _) ->
                        indicesNames.contains(name)
                    }
                    .forEach {
                        val indexUUID = defaultIndexMetadataService.getIndexUUID(it.value)
                        indexUuidToIndexMetadata[indexUUID] = it.value
                    }

                processResponse()
            } catch (e: Exception) {
                actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
            }
        }

        private fun processResponse() {
            val mReq = MultiGetRequest()
            indicesToRetry.map { it.key }.forEach { mReq.add(INDEX_MANAGEMENT_INDEX, it) }

            pluginClient.multiGet(
                mReq,
                object : ActionListener<MultiGetResponse> {
                    override fun onResponse(response: MultiGetResponse) {
                        // config index may not be initialized
                        val f = response.responses.first()
                        if (f.isFailed && f.failure.failure is IndexNotFoundException) {
                            indicesToRetry.forEach { (uuid, name) ->
                                failedIndices.add(FailedIndex(name, uuid, "This index is not being managed."))
                            }
                            actionListener.onResponse(ISMStatusResponse(0, failedIndices))
                            return
                        }

                        response.forEach {
                            indicesManagedState[it.id] = it.response.isExists
                        }

                        // get back metadata from config index
                        pluginClient.multiGet(
                            buildMgetMetadataRequest(indicesToRetry.toList().map { it.first }),
                            ActionListener.wrap(::onMgetMetadataResponse, ::onFailure),
                        )
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                },
            )
        }

        @Suppress("ComplexMethod")
        private fun onMgetMetadataResponse(mgetResponse: MultiGetResponse) {
            val metadataMap = mgetResponseToMap(mgetResponse)
            indicesToRetry.forEach { (indexUuid, indexName) ->
                val mgetFailure = metadataMap[managedIndexMetadataID(indexUuid)]?.second
                val managedIndexMetadata: ManagedIndexMetaData? = metadataMap[managedIndexMetadataID(indexUuid)]?.first
                when {
                    indicesManagedState[indexUuid] == false ->
                        failedIndices.add(FailedIndex(indexName, indexUuid, "This index is not being managed."))
                    mgetFailure != null ->
                        failedIndices.add(FailedIndex(indexName, indexUuid, "Failed to get managed index metadata, $mgetFailure"))
                    managedIndexMetadata == null -> {
                        failedIndices.add(FailedIndex(indexName, indexUuid, "This index has no metadata information"))
                    }
                    !managedIndexMetadata.isFailed ->
                        failedIndices.add(FailedIndex(indexName, indexUuid, "This index is not in failed state."))
                    else ->
                        listOfMetadata.add(managedIndexMetadata)
                }
            }

            if (listOfMetadata.isNotEmpty()) {
                bulkEnableJob(listOfMetadata.map { it.indexUuid })
            } else {
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
            }
        }

        private fun bulkEnableJob(jobDocIds: List<String>) {
            val requestsToRetry = jobDocIds.map { updateEnableManagedIndexRequest(it) }
            val bulkRequest = BulkRequest().add(requestsToRetry)

            client.bulk(bulkRequest, ActionListener.wrap(::onEnableJobBulkResponse, ::onFailure))
        }

        private fun onEnableJobBulkResponse(bulkResponse: BulkResponse) {
            for (bulkItemResponse in bulkResponse) {
                val managedIndexMetaData = listOfMetadata.first { it.indexUuid == bulkItemResponse.id }
                if (bulkItemResponse.isFailed) {
                    failedIndices.add(FailedIndex(managedIndexMetaData.index, managedIndexMetaData.indexUuid, bulkItemResponse.failureMessage))
                } else {
                    listOfIndexToMetadata.add(
                        Pair(
                            Index(managedIndexMetaData.index, managedIndexMetaData.indexUuid),
                            managedIndexMetaData.copy(
                                stepMetaData = null,
                                policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
                                actionMetaData =
                                managedIndexMetaData.actionMetaData?.copy(
                                    failed = false,
                                    consumedRetries = 0,
                                    lastRetryTime = null,
                                    startTime = null,
                                ),
                                transitionTo = request.startState,
                                info = mapOf("message" to "Pending retry of failed managed index"),
                            ),
                        ),
                    )
                }
            }

            if (listOfIndexToMetadata.isNotEmpty()) {
                listOfIndexToMetadata.forEachIndexed { ind, (index, _) ->
                    mapOfItemIdToIndex[ind] = index
                }

                val updateMetadataRequests =
                    listOfIndexToMetadata.map { (index, metadata) ->
                        val builder = metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS, true)
                        UpdateRequest(INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(index.uuid)).routing(index.uuid).doc(builder)
                    }
                val bulkUpdateMetadataRequest = BulkRequest().add(updateMetadataRequests)
                bulkUpdateMetadataRequest.refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE

                pluginClient.bulk(bulkUpdateMetadataRequest, ActionListener.wrap(::onBulkUpdateMetadataResponse, ::onFailure))
            } else {
                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
            }
        }

        private fun onBulkUpdateMetadataResponse(bulkResponse: BulkResponse) {
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            failedResponses.forEach {
                val index = mapOfItemIdToIndex[it.itemId]
                if (index != null) {
                    failedIndices.add(FailedIndex(index.name, index.uuid, "Failed to update metadata for index ${index.name}"))
                }
            }

            val updated = (bulkResponse.items ?: arrayOf()).size - failedResponses.size
            actionListener.onResponse(ISMStatusResponse(updated, failedIndices))
        }

        fun onFailure(e: Exception) {
            try {
                if (e is ClusterBlockException) {
                    failedIndices.addAll(
                        listOfIndexToMetadata.map {
                            FailedIndex(it.first.name, it.first.uuid, "Failed to update due to ClusterBlockException. ${e.message}")
                        },
                    )
                }

                actionListener.onResponse(ISMStatusResponse(0, failedIndices))
            } catch (inner: Exception) {
                inner.addSuppressed(e)
                log.error("Failed to send failure response", inner)
            }
        }
    }
}
