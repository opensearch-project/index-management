/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.support.master.TransportMasterNodeAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.ClusterStateTaskConfig
import org.opensearch.cluster.ClusterStateTaskExecutor
import org.opensearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult
import org.opensearch.cluster.ClusterStateTaskListener
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Priority
import org.opensearch.common.inject.Inject
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.index.Index
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

class TransportUpdateManagedIndexMetaDataAction @Inject constructor(
    threadPool: ThreadPool,
    clusterService: ClusterService,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val indexMetadataProvider: IndexMetadataProvider,
    indexNameExpressionResolver: IndexNameExpressionResolver
) : TransportMasterNodeAction<UpdateManagedIndexMetaDataRequest, AcknowledgedResponse>(
    UpdateManagedIndexMetaDataAction.INSTANCE.name(),
    transportService,
    clusterService,
    threadPool,
    actionFilters,
    Writeable.Reader { UpdateManagedIndexMetaDataRequest(it) },
    indexNameExpressionResolver
) {

    private val log = LogManager.getLogger(javaClass)
    private val executor = ManagedIndexMetaDataExecutor()

    override fun checkBlock(request: UpdateManagedIndexMetaDataRequest, state: ClusterState): ClusterBlockException? {
        // https://github.com/elastic/elasticsearch/commit/ae14b4e6f96b554ca8f4aaf4039b468f52df0123
        // This commit will help us to give each individual index name and the error that is cause it. For now it will be a generic error message.
        val indicesToAddTo = request.indicesToAddManagedIndexMetaDataTo.map { it.first }.toTypedArray()
        val indicesToRemoveFrom = request.indicesToRemoveManagedIndexMetaDataFrom.map { it }.toTypedArray()
        val indices = checkExtensionsOverrideBlock(indicesToAddTo + indicesToRemoveFrom, state)

        return state.blocks.indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices)
    }

    /*
     * Index Management extensions may provide an index setting, which, if set to true, overrides the cluster metadata write block
     */
    private fun checkExtensionsOverrideBlock(indices: Array<Index>, state: ClusterState): Array<String> {
        val indexBlockOverrideSettings = indexMetadataProvider.getIndexMetadataWriteOverrideSettings()
        val indicesToBlock = indices.toMutableList()
        indexBlockOverrideSettings.forEach { indexBlockOverrideSetting ->
            indicesToBlock.removeIf { state.metadata.getIndexSafe(it).settings.getAsBoolean(indexBlockOverrideSetting, false) }
        }
        return indicesToBlock
            .map { it.name }
            .toTypedArray()
    }

    override fun masterOperation(
        request: UpdateManagedIndexMetaDataRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        clusterService.submitStateUpdateTask(
            IndexManagementPlugin.OLD_PLUGIN_NAME,
            ManagedIndexMetaDataTask(request.indicesToAddManagedIndexMetaDataTo, request.indicesToRemoveManagedIndexMetaDataFrom),
            ClusterStateTaskConfig.build(Priority.NORMAL),
            executor,
            object : ClusterStateTaskListener {
                override fun onFailure(source: String, e: Exception) = listener.onFailure(e)

                override fun clusterStateProcessed(source: String, oldState: ClusterState, newState: ClusterState) =
                    listener.onResponse(AcknowledgedResponse(true))
            }
        )
    }

    override fun read(si: StreamInput): AcknowledgedResponse {
        return AcknowledgedResponse(si)
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    inner class ManagedIndexMetaDataExecutor : ClusterStateTaskExecutor<ManagedIndexMetaDataTask> {

        override fun execute(currentState: ClusterState, tasks: List<ManagedIndexMetaDataTask>): ClusterTasksResult<ManagedIndexMetaDataTask> {
            val newClusterState = getUpdatedClusterState(currentState, tasks)
            return ClusterTasksResult.builder<ManagedIndexMetaDataTask>().successes(tasks).build(newClusterState)
        }
    }

    fun getUpdatedClusterState(currentState: ClusterState, tasks: List<ManagedIndexMetaDataTask>): ClusterState {
        // If there are no indices to make changes to, return early.
        // Also doing this because when creating a metaDataBuilder and making no changes to it, for some
        // reason the task does not complete, leading to indefinite suspension.
        if (tasks.all { it.indicesToAddManagedIndexMetaDataTo.isEmpty() && it.indicesToRemoveManagedIndexMetaDataFrom.isEmpty() }
        ) {
            return currentState
        }
        log.trace("Start of building new cluster state")
        val metaDataBuilder = Metadata.builder(currentState.metadata)
        for (task in tasks) {
            for (pair in task.indicesToAddManagedIndexMetaDataTo) {
                if (currentState.metadata.hasIndex(pair.first.name)) {
                    metaDataBuilder.put(
                        IndexMetadata.builder(currentState.metadata.index(pair.first))
                            .putCustom(ManagedIndexMetaData.MANAGED_INDEX_METADATA_TYPE, pair.second.toMap())
                    )
                } else {
                    log.debug("No IndexMetadata found for [${pair.first.name}] when updating ManagedIndexMetaData")
                }
            }

            for (index in task.indicesToRemoveManagedIndexMetaDataFrom) {
                if (currentState.metadata.hasIndex(index.name)) {
                    val indexMetaDataBuilder = IndexMetadata.builder(currentState.metadata.index(index))
                    indexMetaDataBuilder.removeCustom(ManagedIndexMetaData.MANAGED_INDEX_METADATA_TYPE)

                    metaDataBuilder.put(indexMetaDataBuilder)
                } else {
                    log.debug("No IndexMetadata found for [${index.name}] when removing ManagedIndexMetaData")
                }
            }
        }
        log.trace("End of building new cluster state")

        return ClusterState.builder(currentState).metadata(metaDataBuilder).build()
    }

    companion object {
        data class ManagedIndexMetaDataTask(
            val indicesToAddManagedIndexMetaDataTo: List<Pair<Index, ManagedIndexMetaData>>,
            val indicesToRemoveManagedIndexMetaDataFrom: List<Index>
        )
    }
}
