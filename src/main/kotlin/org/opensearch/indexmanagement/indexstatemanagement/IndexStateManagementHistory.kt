/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.rollover.RolloverRequest
import org.opensearch.action.admin.indices.rollover.RolloverResponse
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.LocalNodeMasterListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_REPLICAS
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
import org.opensearch.indexmanagement.opensearchapi.OPENDISTRO_SECURITY_PROTECTED_INDICES_CONF_REQUEST
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import java.time.Instant

@OpenForTesting
@Suppress("TooManyFunctions")
class IndexStateManagementHistory(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
    private val indexManagementIndices: IndexManagementIndices
) : LocalNodeMasterListener {

    private val logger = LogManager.getLogger(javaClass)
    private var scheduledRollover: Scheduler.Cancellable? = null

    @Volatile private var historyEnabled = ManagedIndexSettings.HISTORY_ENABLED.get(settings)
    @Volatile private var historyMaxDocs = ManagedIndexSettings.HISTORY_MAX_DOCS.get(settings)
    @Volatile private var historyMaxAge = ManagedIndexSettings.HISTORY_INDEX_MAX_AGE.get(settings)
    @Volatile private var historyRolloverCheckPeriod = ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD.get(settings)
    @Volatile private var historyRetentionPeriod = ManagedIndexSettings.HISTORY_RETENTION_PERIOD.get(settings)
    @Volatile private var historyNumberOfShards = ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS.get(settings)
    @Volatile private var historyNumberOfReplicas = ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS.get(settings)

    init {
        clusterService.addLocalNodeMasterListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_ENABLED) {
            historyEnabled = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_MAX_DOCS) { historyMaxDocs = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_INDEX_MAX_AGE) { historyMaxAge = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD) {
            historyRolloverCheckPeriod = it
            rescheduleRollover()
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_RETENTION_PERIOD) {
            historyRetentionPeriod = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS) {
            historyNumberOfShards = it
        }
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS) {
            historyNumberOfReplicas = it
        }
    }

    override fun onMaster() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            if (historyEnabled) rolloverHistoryIndex()
            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool.scheduleWithFixedDelay(
                { rolloverAndDeleteHistoryIndex() },
                historyRolloverCheckPeriod, ThreadPool.Names.MANAGEMENT
            )
        } catch (e: Exception) {
            // This should be run on cluster startup
            logger.error("Error creating ISM history index.", e)
        }
    }

    override fun offMaster() {
        scheduledRollover?.cancel()
    }

    private fun rescheduleRollover() {
        if (clusterService.state().nodes.isLocalNodeElectedMaster) {
            scheduledRollover?.cancel()
            scheduledRollover = threadPool.scheduleWithFixedDelay(
                { rolloverAndDeleteHistoryIndex() },
                historyRolloverCheckPeriod, ThreadPool.Names.MANAGEMENT
            )
        }
    }

    private fun rolloverAndDeleteHistoryIndex() {
        val ctx = threadPool.threadContext.stashContext()
        try {
            if (threadPool.threadContext.getTransient<String?>(OPENDISTRO_SECURITY_PROTECTED_INDICES_CONF_REQUEST) == null) {
                threadPool.threadContext.putTransient(OPENDISTRO_SECURITY_PROTECTED_INDICES_CONF_REQUEST, "true")
            }
            if (historyEnabled) rolloverHistoryIndex()
            deleteOldHistoryIndex()
        } finally {
            ctx.close()
        }
    }

    private fun rolloverHistoryIndex() {
        if (!indexManagementIndices.indexStateManagementIndexHistoryExists()) {
            return
        }

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        val request = RolloverRequest(IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS, null)
        request.createIndexRequest.index(IndexManagementIndices.HISTORY_INDEX_PATTERN)
            .mapping(IndexManagementIndices.indexStateManagementHistoryMappings)
            .settings(
                Settings.builder()
                    .put(INDEX_HIDDEN, true)
                    .put(INDEX_NUMBER_OF_SHARDS, historyNumberOfShards)
                    .put(INDEX_NUMBER_OF_REPLICAS, historyNumberOfReplicas)
            )
        request.addMaxIndexDocsCondition(historyMaxDocs)
        request.addMaxIndexAgeCondition(historyMaxAge)
        client.admin().indices().rolloverIndex(
            request,
            object : ActionListener<RolloverResponse> {
                override fun onResponse(response: RolloverResponse) {
                    if (response.isRolledOver) {
                        logger.info("${IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS} rolled over.")
                    } else {
                        logger.info(
                            "${IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS} not rolled over. " +
                                "Conditions were: ${response.conditionStatus}"
                        )
                    }
                }

                override fun onFailure(e: Exception) {
                    logger.error("${IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS} roll over failed.", e)
                }
            }
        )
    }

    @Suppress("SpreadOperator", "NestedBlockDepth", "ComplexMethod")
    private fun deleteOldHistoryIndex() {

        val clusterStateRequest = ClusterStateRequest()
            .clear()
            .indices(IndexManagementIndices.HISTORY_ALL)
            .metadata(true)
            .local(true)
            .indicesOptions(IndicesOptions.strictExpand())

        client.admin().cluster().state(
            clusterStateRequest,
            object : ActionListener<ClusterStateResponse> {
                override fun onResponse(clusterStateResponse: ClusterStateResponse) {
                    if (!clusterStateResponse.state.metadata.indices.isEmpty) {
                        val indicesToDelete = getIndicesToDelete(clusterStateResponse)
                        logger.info("Deleting old history indices viz $indicesToDelete")
                        deleteAllOldHistoryIndices(indicesToDelete)
                    } else {
                        logger.info("No Old History Indices to delete")
                    }
                }

                override fun onFailure(exception: Exception) {
                    logger.error("Error fetching cluster state ${exception.message}")
                }
            }
        )
    }

    private fun getIndicesToDelete(clusterStateResponse: ClusterStateResponse): List<String> {
        val indicesToDelete = mutableListOf<String>()
        for (entry in clusterStateResponse.state.metadata.indices()) {
            val indexMetaData = entry.value
            val creationTime = indexMetaData.creationDate

            if ((Instant.now().toEpochMilli() - creationTime) > historyRetentionPeriod.millis) {
                val alias = indexMetaData.aliases.firstOrNull { IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS == it.value.alias }
                if (alias != null && historyEnabled) {
                    // If index has write alias and history is enable, don't delete the index.
                    continue
                }

                indicesToDelete.add(indexMetaData.index.name)
            }
        }
        return indicesToDelete
    }

    @Suppress("SpreadOperator")
    private fun deleteAllOldHistoryIndices(indicesToDelete: List<String>) {
        if (indicesToDelete.isNotEmpty()) {
            val deleteRequest = DeleteIndexRequest(*indicesToDelete.toTypedArray())
            client.admin().indices().delete(
                deleteRequest,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(deleteIndicesResponse: AcknowledgedResponse) {
                        if (!deleteIndicesResponse.isAcknowledged) {
                            logger.error("could not delete one or more ISM history index. $indicesToDelete. Retrying one by one.")
                            deleteOldHistoryIndex(indicesToDelete)
                        }
                    }
                    override fun onFailure(exception: Exception) {
                        logger.error("Error deleting old history indices ${exception.message}")
                        deleteOldHistoryIndex(indicesToDelete)
                    }
                }
            )
        }
    }

    @Suppress("SpreadOperator")
    private fun deleteOldHistoryIndex(indicesToDelete: List<String>) {
        for (index in indicesToDelete) {
            val singleDeleteRequest = DeleteIndexRequest(*indicesToDelete.toTypedArray())
            client.admin().indices().delete(
                singleDeleteRequest,
                object : ActionListener<AcknowledgedResponse> {
                    override fun onResponse(singleDeleteResponse: AcknowledgedResponse) {
                        if (!singleDeleteResponse.isAcknowledged) {
                            logger.error("could not delete one or more ISM history index. $index.")
                        }
                    }
                    override fun onFailure(exception: Exception) {
                        logger.debug("Exception ${exception.message} while deleting the index $index")
                    }
                }
            )
        }
    }

    @Suppress("NestedBlockDepth")
    suspend fun addManagedIndexMetaDataHistory(managedIndexMetaData: List<ManagedIndexMetaData>) {
        if (!historyEnabled) {
            logger.debug("Index State Management history is not enabled")
            return
        }

        if (!indexManagementIndices.checkAndUpdateHistoryIndex()) {
            logger.error("Failed to create or update the ism history index:")
            return // we can't continue to add the history documents below as it would potentially create dynamic mappings
        }

        val docWriteRequest: List<DocWriteRequest<*>> = managedIndexMetaData
            .filter { shouldAddManagedIndexMetaDataToHistory(it) }
            .map { createManagedIndexMetaDataHistoryIndexRequest(it) }

        if (docWriteRequest.isNotEmpty()) {
            val bulkRequest = BulkRequest().add(docWriteRequest)

            try {
                val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }

                for (bulkItemResponse in bulkResponse) {
                    if (bulkItemResponse.isFailed) {
                        logger.error("Failed to add history. Id: ${bulkItemResponse.id}, failureMessage: ${bulkItemResponse.failureMessage}")
                    }
                }
            } catch (e: Exception) {
                logger.error("failed to index indexMetaData History.", e)
            }
        }
    }

    private fun shouldAddManagedIndexMetaDataToHistory(managedIndexMetaData: ManagedIndexMetaData): Boolean {
        return when (managedIndexMetaData.stepMetaData?.stepStatus) {
            Step.StepStatus.STARTING -> false
            Step.StepStatus.CONDITION_NOT_MET -> false
            else -> true
        }
    }

    private fun createManagedIndexMetaDataHistoryIndexRequest(managedIndexMetaData: ManagedIndexMetaData): IndexRequest {
        val builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(IndexManagementPlugin.INDEX_STATE_MANAGEMENT_HISTORY_TYPE)
        managedIndexMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder
            .field("history_timestamp", Instant.now().toEpochMilli())
            .endObject()
            .endObject()
        return IndexRequest(IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS)
            .source(builder)
    }
}
