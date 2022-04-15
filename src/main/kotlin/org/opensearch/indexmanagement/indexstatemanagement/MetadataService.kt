/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.ActionListener
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkItemResponse
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.Index
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getManagedIndexMetadata
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.revertManagedIndexMetadataID
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.util.IndexManagementException
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.rest.RestStatus
import java.lang.Exception

/**
 * When all nodes have same version IM plugin (CDI/DDI finished)
 * MetadataService starts to move metadata from cluster state to config index
 */
@OpenForTesting
@Suppress("MagicNumber", "ReturnCount", "LongMethod", "ComplexMethod")
class MetadataService(
    private val client: Client,
    private val clusterService: ClusterService,
    private val skipExecution: SkipExecution,
    private val imIndices: IndexManagementIndices
) {
    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var runningLock = false // in case 2 moveMetadata() process running

    private val successfullyIndexedIndices = mutableSetOf<MetadataDocID>()
    private var failedToIndexIndices = mutableMapOf<MetadataDocID, BulkItemResponse.Failure>()
    private var failedToCleanIndices = mutableSetOf<Index>()

    private var counter = 0
    final var runTimeCounter = 1
        private set
    private val maxRunTime = 10

    // used in coordinator sweep to cancel scheduled process
    @Volatile final var finishFlag = false
        private set
    fun reenableMetadataService() { finishFlag = false }

    @Volatile private var retryPolicy =
        BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(50), 3)

    suspend fun moveMetadata() {
        if (runningLock) {
            logger.info("There is a move metadata process running...")
            return
        } else if (finishFlag) {
            logger.info("Move metadata has finished.")
            return
        }
        try {
            runningLock = true

            if (skipExecution.flag) {
                logger.info("Cluster still has nodes running old version of ISM plugin, skip ping execution on new nodes until all nodes upgraded")
                runningLock = false
                return
            }

            if (!imIndices.indexManagementIndexExists()) {
                logger.info("ISM config index not exist, so we cancel the metadata migration job.")
                finishFlag = true; runningLock = false; runTimeCounter = 0
                return
            }

            if (runTimeCounter > maxRunTime) {
                updateStatusSetting(-1)
                finishFlag = true; runningLock = false; runTimeCounter = 0
                return
            }
            logger.info("Doing metadata migration $runTimeCounter time.")

            val indicesMetadata = clusterService.state().metadata.indices
            var clusterStateManagedIndexMetadata = indicesMetadata.map {
                it.key to it.value.getManagedIndexMetadata()
            }.filter { it.second != null }.distinct().toMap()
            // filter out previous failedToClean indices which already been indexed
            clusterStateManagedIndexMetadata =
                clusterStateManagedIndexMetadata.filter { it.key !in failedToCleanIndices.map { index -> index.name } }

            // filter out cluster state metadata with outdated index uuid
            val corruptManagedIndices = mutableListOf<Index>()
            val indexUuidMap = mutableMapOf<IndexUuid, IndexName>()
            clusterStateManagedIndexMetadata.forEach { (indexName, metadata) ->
                val indexMetadata = indicesMetadata[indexName]
                val currentIndexUuid = indexMetadata.indexUUID
                if (currentIndexUuid != metadata?.indexUuid) {
                    corruptManagedIndices.add(indexMetadata.index)
                } else {
                    indexUuidMap[currentIndexUuid] = indexName
                }
            }
            logger.info("Corrupt managed indices with outdated index uuid in metadata: $corruptManagedIndices")
            clusterStateManagedIndexMetadata = clusterStateManagedIndexMetadata.filter { (indexName, _) ->
                indexName !in corruptManagedIndices.map { it.name }
            }

            if (clusterStateManagedIndexMetadata.isEmpty()) {
                if (counter++ > 2 && corruptManagedIndices.isEmpty()) {
                    logger.info("Move Metadata succeed, set finish flag to true. Indices failed to get indexed: $failedToIndexIndices")
                    updateStatusSetting(1)
                    finishFlag = true; runningLock = false; runTimeCounter = 0
                    return
                }
                if (failedToCleanIndices.isNotEmpty()) {
                    logger.info("Failed to clean indices: $failedToCleanIndices. Only clean cluster state metadata in this run.")
                    cleanMetadatas(failedToCleanIndices.toList())
                    finishFlag = false; runningLock = false
                    return
                }
            } else {
                counter = 0; finishFlag = false // index metadata for indices which metadata hasn't been indexed
                val bulkIndexReq =
                    clusterStateManagedIndexMetadata.mapNotNull { it.value }.map {
                        managedIndexMetadataIndexRequest(
                            it,
                            waitRefresh = false, // should be set at bulk request level
                            create = true // restrict this as create operation
                        )
                    }
                // remove the part which gonna be indexed from last time failedToIndex
                failedToIndexIndices = failedToIndexIndices.filterKeys { it !in indexUuidMap.keys }.toMutableMap()
                successfullyIndexedIndices.clear()
                indexMetadatas(bulkIndexReq)

                logger.info("success indexed: ${successfullyIndexedIndices.map { indexUuidMap[it] }}")
                logger.info(
                    "failed indexed: ${failedToIndexIndices.map { indexUuidMap[it.key] }};" +
                        "failed reason: ${failedToIndexIndices.values.distinct()}"
                )
            }

            // clean metadata for indices which metadata already been indexed
            val indicesToCleanMetadata =
                indexUuidMap.filter { it.key in successfullyIndexedIndices }.map { Index(it.value, it.key) }
                    .toList() + failedToCleanIndices + corruptManagedIndices

            cleanMetadatas(indicesToCleanMetadata)
            if (failedToCleanIndices.isNotEmpty()) {
                logger.info("Failed to clean cluster metadata for: ${failedToCleanIndices.map { it.name }}")
            }

            runTimeCounter++
        } finally {
            runningLock = false
        }
    }

    private suspend fun updateStatusSetting(status: Int) {
        val newSetting = Settings.builder().put(ManagedIndexSettings.METADATA_SERVICE_STATUS.key, status)
        val request = ClusterUpdateSettingsRequest().persistentSettings(newSetting)
        retryPolicy.retry(logger, listOf(RestStatus.INTERNAL_SERVER_ERROR)) {
            client.admin().cluster().updateSettings(request, updateSettingListener(status))
        }
    }

    private fun updateSettingListener(status: Int): ActionListener<ClusterUpdateSettingsResponse> {
        return object : ActionListener<ClusterUpdateSettingsResponse> {
            override fun onFailure(e: Exception) {
                logger.error("Failed to update template migration setting to $status", e)
                throw IndexManagementException.wrap(Exception("Failed to update template migration setting to $status"))
            }

            override fun onResponse(response: ClusterUpdateSettingsResponse) {
                if (!response.isAcknowledged) {
                    logger.error("Update template migration setting to $status is not acknowledged")
                    throw IndexManagementException.wrap(
                        Exception("Update template migration setting to $status is not acknowledged")
                    )
                } else {
                    logger.info("Successfully update template migration setting to $status")
                }
            }
        }
    }

    private suspend fun indexMetadatas(requests: List<DocWriteRequest<*>>) {
        if (requests.isEmpty()) return
        var requestsToRetry = requests

        // when we try to index sth to config index
        // we need to make sure the schema is up to date
        if (!imIndices.attemptUpdateConfigIndexMapping()) {
            logger.error("Failed to update config index mapping.")
            return
        }

        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkRequest().add(requestsToRetry)
            val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }

            val retryIndexUuids = mutableListOf<Int>()
            bulkResponse.items.forEach {
                val indexUuid = revertManagedIndexMetadataID(it.id)
                if (it.isFailed) {
                    if (it.status() == RestStatus.TOO_MANY_REQUESTS) {
                        retryIndexUuids.add(it.itemId)
                    } else {
                        logger.error("failed reason: ${it.failure}, ${it.failureMessage}")
                        failedToIndexIndices[indexUuid] = it.failure
                    }
                } else {
                    successfullyIndexedIndices.add(indexUuid)
                    failedToIndexIndices.remove(indexUuid)
                }
            }
            requestsToRetry = retryIndexUuids.map { bulkRequest.requests()[it] }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToOpenSearchException(retryCause)
            }
        }
    }

    private suspend fun cleanMetadatas(indices: List<Index>) {
        if (indices.isEmpty()) return

        val request = UpdateManagedIndexMetaDataRequest(indicesToRemoveManagedIndexMetaDataFrom = indices)
        try {
            retryPolicy.retry(logger) {
                val response: AcknowledgedResponse =
                    client.suspendUntil { execute(UpdateManagedIndexMetaDataAction.INSTANCE, request, it) }
                if (response.isAcknowledged) {
                    failedToCleanIndices.removeAll(indices)
                } else {
                    logger.error("Failed to clean cluster state metadata for indices: [$indices].")
                    failedToCleanIndices.addAll(indices)
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to clean cluster state metadata for indices: [$indices].", e)
            failedToCleanIndices.addAll(indices)
        }
    }
}

typealias MetadataDocID = String
typealias IndexUuid = String
typealias IndexName = String
