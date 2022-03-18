/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
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
            info = mapOf("message" to "Metadata not properly populated")
            stepStatus = StepStatus.FAILED
            return this
        }
        try {
            val indexStatsRequests: IndicesStatsRequest = IndicesStatsRequest().indices(indexName)
            val response: IndicesStatsResponse = context.client.admin().indices().suspendUntil { stats(indexStatsRequests, it) }
            val numPrimaryShards = context.clusterService.state().metadata.indices[indexName].numberOfShards
            val nodeToMoveOnto = shrinkActionProperties.nodeName
            var numShardsOnNode = 0
            val shardToCheckpointSetMap: MutableMap<ShardId, MutableSet<Long>> = mutableMapOf()
            for (shard: ShardStats in response.shards) {
                val seqNoStats = shard.seqNoStats
                val routingInfo = shard.shardRouting
                if (seqNoStats != null) {
                    val checkpoint = seqNoStats.localCheckpoint
                    val shardId = shard.shardRouting.shardId()
                    val checkpointsOfShard = shardToCheckpointSetMap.getOrDefault(shardId, mutableSetOf())
                    checkpointsOfShard.add(checkpoint)
                    shardToCheckpointSetMap[shardId] = checkpointsOfShard
                }
                // TODO: Test if we can make this appear / if we can, fail the action.
                shardToCheckpointSetMap.entries.forEach {
                    (_, checkpointSet) ->
                    if (checkpointSet.size > 1) {
                        logger.warn("There are shards with varying local checkpoints")
                    }
                }
                val nodeIdShardIsOn = routingInfo.currentNodeId()
                val nodeShardIsOn = context.clusterService.state().nodes()[nodeIdShardIsOn].name
                if (nodeShardIsOn.equals(nodeToMoveOnto) && routingInfo.started()) {
                    numShardsOnNode++
                }
            }
            if (numShardsOnNode >= numPrimaryShards) {
                info = mapOf("message" to getSuccessMessage(nodeToMoveOnto))
                stepStatus = StepStatus.COMPLETED
                return this
            }
            val numShardsLeft = numPrimaryShards - numShardsOnNode
            checkTimeOut(context, shrinkActionProperties, numShardsLeft, nodeToMoveOnto)
            return this
        } catch (e: RemoteTransportException) {
            releaseShrinkLock(shrinkActionProperties, context.jobContext, logger)
            info = mapOf("message" to FAILURE_MESSAGE)
            stepStatus = StepStatus.FAILED
            return this
        } catch (e: Exception) {
            releaseShrinkLock(shrinkActionProperties, context.jobContext, logger)
            info = mapOf("message" to FAILURE_MESSAGE, "cause" to "{${e.message}}")
            stepStatus = StepStatus.FAILED
            return this
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
        shrinkActionProperties: ShrinkActionProperties,
        numShardsLeft: Int,
        nodeToMoveOnto: String
    ) {
        val managedIndexMetadata = stepContext.metadata
        val indexName = managedIndexMetadata.index
        val timeFromActionStarted: Duration = Duration.between(getActionStartTime(managedIndexMetadata), Instant.now())
        val timeOutInSeconds = action.configTimeout?.timeout?.seconds ?: MOVE_SHARDS_TIMEOUT_IN_SECONDS
        // Get ActionTimeout if given, otherwise use default timeout of 12 hours
        stepStatus = if (timeFromActionStarted.toSeconds() > timeOutInSeconds) {
            logger.debug(
                "Move shards failing on [$indexName] because" +
                    " [$numShardsLeft] shards still needing to be moved"
            )
            if (managedIndexMetadata.actionMetaData?.actionProperties?.shrinkActionProperties != null) {
                releaseShrinkLock(shrinkActionProperties, stepContext.jobContext, logger)
            }
            info = mapOf("message" to getTimeoutFailure(nodeToMoveOnto))
            StepStatus.FAILED
        } else {
            logger.debug(
                "Move shards still running on [$indexName] with" +
                    " [$numShardsLeft] shards still needing to be moved"
            )
            info = mapOf("message" to getTimeoutDelay(nodeToMoveOnto))
            StepStatus.CONDITION_NOT_MET
        }
    }

    override fun isIdempotent() = true

    companion object {
        const val name = "wait_for_move_shards_step"
        fun getSuccessMessage(node: String) = "The shards successfully moved to $node."
        fun getTimeoutFailure(node: String) = "Shrink failed because it took to long to move shards to $node"
        fun getTimeoutDelay(node: String) = "Shrink delayed because it took to long to move shards to $node"
        const val FAILURE_MESSAGE = "Shrink failed when waiting for shards to move."
        const val MOVE_SHARDS_TIMEOUT_IN_SECONDS = 43200L // 12hrs in seconds
        const val RESOURCE_NAME = "node_name"
        const val RESOURCE_TYPE = "shrink"
    }
}
