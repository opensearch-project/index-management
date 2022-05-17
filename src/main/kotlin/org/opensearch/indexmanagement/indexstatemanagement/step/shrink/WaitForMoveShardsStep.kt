/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import java.time.Duration
import java.time.Instant

class WaitForMoveShardsStep(private val action: ShrinkAction) : ShrinkStep(name, true, true, false) {

    @Suppress("ReturnCount")
    override suspend fun wrappedExecute(context: StepContext): WaitForMoveShardsStep {
        val indexName = context.metadata.index
        // If the returned shrinkActionProperties are null, then the status has been set to failed, just return
        val localShrinkActionProperties = updateAndGetShrinkActionProperties(context) ?: return this

        val shardStats = getShardStats(indexName, context.client) ?: return this

        val numShardsInSync = getNumShardsInSync(shardStats, context.clusterService.state(), indexName)
        val nodeToMoveOnto = localShrinkActionProperties.nodeName
        val numShardsOnNode = getNumShardsWithCopyOnNode(shardStats, context.clusterService.state(), nodeToMoveOnto)
        val numPrimaryShards = context.clusterService.state().metadata.indices[indexName].numberOfShards

        // If a copy of each shard is on the node, and all shards are in sync, move on
        if (numShardsOnNode >= numPrimaryShards && numShardsInSync >= numPrimaryShards) {
            info = mapOf("message" to getSuccessMessage(nodeToMoveOnto))
            stepStatus = StepStatus.COMPLETED
        } else {
            val numShardsNotOnNode = numPrimaryShards - numShardsOnNode
            val numShardsNotInSync = numPrimaryShards - numShardsInSync
            checkTimeOut(context, numShardsNotOnNode, numShardsNotInSync, nodeToMoveOnto)
        }
        return this
    }

    // Returns the number of shard IDs where all primary and replicas are in sync
    private fun getNumShardsInSync(shardStats: Array<ShardStats>, state: ClusterState, indexName: String): Int {
        val numReplicas = state.metadata.indices[indexName].numberOfReplicas
        val inSyncAllocations = state.metadata.indices[indexName].inSyncAllocationIds
        var numShardsInSync = 0
        for (shard: ShardStats in shardStats) {
            val routingInfo = shard.shardRouting
            // Only check primaries so that we only check once for each shardID
            if (routingInfo.primary()) {
                // All shards must be in sync as it isn't known which shard (replica or primary) will be
                // moved to the target node and used in the shrink.
                if (inSyncAllocations[routingInfo.id].size == (numReplicas + 1)) {
                    numShardsInSync++
                }
            }
        }
        return numShardsInSync
    }

    // Returns the number of shard IDs which have either a primary or replica on the target node
    @Suppress("LoopWithTooManyJumpStatements")
    private fun getNumShardsWithCopyOnNode(shardStats: Array<ShardStats>, clusterState: ClusterState, nodeToMoveOnto: String): Int {
        val shardIdStartedOnNode: MutableMap<Int, Boolean> = mutableMapOf()
        for (shard in shardStats) {
            val routingInfo = shard.shardRouting
            val shardId = routingInfo.shardId().id
            // If we have already confirmed a copy of the shard is on the node and started, move on
            if (shardIdStartedOnNode[shardId] == true) continue
            // If nodeName is null, then the nodes could have changed since the indicesStatsResponse, just skip adding it
            val nodeName: String = clusterState.nodes[routingInfo.currentNodeId()].name ?: continue
            shardIdStartedOnNode[shardId] = nodeName == nodeToMoveOnto && routingInfo.started()
        }
        return shardIdStartedOnNode.values.count { it }
    }

    private suspend fun getShardStats(indexName: String, client: Client): Array<ShardStats>? {
        val indexStatsRequests: IndicesStatsRequest = IndicesStatsRequest().indices(indexName)
        val response: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(indexStatsRequests, it) }
        val shardStats = response.shards
        if (shardStats == null) {
            fail(AttemptMoveShardsStep.FAILURE_MESSAGE, "Failed to move shards in shrink action as shard stats were null.")
            return null
        }
        return shardStats
    }

    override fun getGenericFailureMessage(): String = FAILURE_MESSAGE

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            actionMetaData = currentMetadata.actionMetaData?.copy(
                actionProperties = ActionProperties(
                    shrinkActionProperties = shrinkActionProperties
                )
            ),
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    private suspend fun checkTimeOut(
        stepContext: StepContext,
        numShardsNotOnNode: Int,
        numShardsNotInSync: Int,
        nodeToMoveOnto: String
    ) {
        val managedIndexMetadata = stepContext.metadata
        val indexName = managedIndexMetadata.index
        val timeSinceActionStarted: Duration = Duration.between(getActionStartTime(managedIndexMetadata), Instant.now())
        val timeOutInSeconds = action.configTimeout?.timeout?.seconds ?: MOVE_SHARDS_TIMEOUT_IN_SECONDS
        // Get ActionTimeout if given, otherwise use default timeout of 12 hours
        if (timeSinceActionStarted.toSeconds() > timeOutInSeconds) {
            cleanupAndFail(getTimeoutFailure(nodeToMoveOnto), getLoggedTimeoutError(indexName, numShardsNotOnNode, numShardsNotInSync))
        } else {
            logger.debug(
                "Shrink action move shards step running on [$indexName], [$numShardsNotOnNode] shards need to be moved, " +
                    "[$numShardsNotInSync] shards need an in sync replica."
            )
            info = mapOf("message" to getTimeoutDelay(nodeToMoveOnto))
            stepStatus = StepStatus.CONDITION_NOT_MET
        }
    }

    override fun isIdempotent() = true

    companion object {
        const val name = "wait_for_move_shards_step"
        fun getSuccessMessage(node: String) = "The shards successfully moved to $node."
        fun getTimeoutFailure(node: String) = "Shrink failed because it took too long to move shards to $node"
        fun getTimeoutDelay(node: String) = "Shrink delayed because it took too long to move shards to $node"
        fun getLoggedTimeoutError(index: String, numShardsNotOnNode: Int, numShardsNotInSync: Int) = "Shrink Action move shards failed on [$index]," +
            " the action timed out with [$numShardsNotOnNode] shards not yet moved and [$numShardsNotInSync] shards without an in sync replica."
        const val FAILURE_MESSAGE = "Shrink failed when waiting for shards to move."
        const val MOVE_SHARDS_TIMEOUT_IN_SECONDS = 43200L // 12hrs in seconds
    }
}
