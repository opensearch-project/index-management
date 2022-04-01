/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.util.clearReadOnlyAndRouting
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.getShrinkLockModel
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.renewShrinkLock
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException
import java.lang.Exception
import java.time.Duration
import java.time.Instant

class WaitForMoveShardsStep(private val action: ShrinkAction) : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount", "NestedBlockDepth")
    override suspend fun execute(): WaitForMoveShardsStep {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val actionMetadata = context.metadata.actionMetaData
        val shrinkActionProperties = actionMetadata?.actionProperties?.shrinkActionProperties
        if (shrinkActionProperties == null) {
            cleanupAndFail(METADATA_FAILURE_MESSAGE)
            return this
        }
        println(getShrinkLockModel(shrinkActionProperties, context.jobContext))
        val lock = renewShrinkLock(shrinkActionProperties, context.jobContext, logger)
        if (lock == null) {
            cleanupAndFail("Failed to renew lock on node [${shrinkActionProperties.nodeName}]")
            return this
        }
        println(lock)
        try {
            val indexStatsRequests: IndicesStatsRequest = IndicesStatsRequest().indices(indexName)
            val response: IndicesStatsResponse = context.client.admin().indices().suspendUntil { stats(indexStatsRequests, it) }
            val numPrimaryShards = context.clusterService.state().metadata.indices[indexName].numberOfShards
            val nodeToMoveOnto = shrinkActionProperties.nodeName
            val inSyncAllocations = context.clusterService.state().metadata.indices[indexName].inSyncAllocationIds
            val numReplicas = context.clusterService.state().metadata.indices[indexName].numberOfReplicas
            var numShardsOnNode = 0
            var numShardsInSync = 0
            for (shard: ShardStats in response.shards) {
                val routingInfo = shard.shardRouting
                val nodeIdShardIsOn = routingInfo.currentNodeId()
                val nodeNameShardIsOn = context.clusterService.state().nodes()[nodeIdShardIsOn].name
                if (routingInfo.primary()) {
                    if (nodeNameShardIsOn.equals(nodeToMoveOnto) && routingInfo.started()) {
                        numShardsOnNode++
                    }
                    // Either there must be no replicas (force unsafe must have been set) or all replicas must be in sync as
                    // it isn't known which shard (any replica or primary) will be moved to the target node and used in the shrink.
                    if (numReplicas == 0 || inSyncAllocations[routingInfo.id].size == (numReplicas + 1)) {
                        numShardsInSync++
                    }
                }
            }
            if (numShardsOnNode >= numPrimaryShards && numShardsInSync >= numPrimaryShards) {
                info = mapOf("message" to getSuccessMessage(nodeToMoveOnto))
                stepStatus = StepStatus.COMPLETED
            } else {
                val numShardsNotOnNode = numPrimaryShards - numShardsOnNode
                val numShardsNotInSync = numPrimaryShards - numShardsInSync
                checkTimeOut(context, numShardsNotOnNode, numShardsNotInSync, nodeToMoveOnto)
            }
            return this
        } catch (e: RemoteTransportException) {
            cleanupAndFail(FAILURE_MESSAGE)
            return this
        } catch (e: Exception) {
            cleanupAndFail(FAILURE_MESSAGE, cause = e.message)
            return this
        }
    }

    // Sets the action to failed, clears the readonly and allocation settings on the source index, and releases the shrink lock
    private suspend fun cleanupAndFail(message: String, cause: String? = null) {
        info = if (cause == null) mapOf("message" to message) else mapOf("message" to message, "cause" to cause)
        stepStatus = StepStatus.FAILED
        val context = this.context ?: return
        try {
            clearReadOnlyAndRouting(context.metadata.index, context.client)
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to clean up routing and readonly setting after a failure: $e")
        }
        try {
            val shrinkActionProperties = context.metadata.actionMetaData?.actionProperties?.shrinkActionProperties ?: return
            releaseShrinkLock(shrinkActionProperties, context.jobContext, logger)
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to release the node lock after a failure: $e")
        }
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        // Saving maxNumSegments in ActionProperties after the force merge operation has begun so that if a ChangePolicy occurred
        // in between this step and WaitForForceMergeStep, a cached segment count expected from the operation is available
        val currentActionMetaData = currentMetadata.actionMetaData
        return currentMetadata.copy(
            actionMetaData = currentActionMetaData?.copy(),
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
            logger.error(
                "Shrink Action move shards failed on [$indexName], the action timed out with [$numShardsNotOnNode] shards not yet " +
                    "moved and [$numShardsNotInSync] shards without an in sync replica."
            )
            cleanupAndFail(getTimeoutFailure(nodeToMoveOnto))
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
        fun getTimeoutFailure(node: String) = "Shrink failed because it took to long to move shards to $node"
        fun getTimeoutDelay(node: String) = "Shrink delayed because it took to long to move shards to $node"
        const val FAILURE_MESSAGE = "Shrink failed when waiting for shards to move."
        const val METADATA_FAILURE_MESSAGE = "Shrink action properties are null, metadata was not properly populated"
        const val MOVE_SHARDS_TIMEOUT_IN_SECONDS = 43200L // 12hrs in seconds
        const val RESOURCE_NAME = "node_name"
        const val RESOURCE_TYPE = "shrink"
    }
}
