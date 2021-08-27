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

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.client.Client
import org.opensearch.cluster.routing.ShardRouting
import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ShrinkActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.transport.RemoteTransportException
import java.time.Duration
import java.time.Instant

class WaitForMoveShardsStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ShrinkActionConfig,
    managedIndexMetaData: ManagedIndexMetaData,
    val context: JobExecutionContext
) : Step(name, managedIndexMetaData) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    @Suppress("TooGenericExceptionCaught", "ComplexMethod")
    override suspend fun execute(): WaitForMoveShardsStep {
        try {
            val indexStatsRequests: IndicesStatsRequest = IndicesStatsRequest().indices(managedIndexMetaData.index)
            val response: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(indexStatsRequests, it) }
            val numPrimaryShards = clusterService.state().metadata.indices[managedIndexMetaData.index].numberOfShards
            if ((managedIndexMetaData.actionMetaData?.actionProperties?.shrinkActionProperties == null) ||
                (managedIndexMetaData.actionMetaData.actionProperties.shrinkActionProperties.nodeName == null)
            ) {
                info = mapOf("message" to "Metadata not properly populated")
                releaseShrinkLock(managedIndexMetaData, context, logger)
                stepStatus = StepStatus.FAILED
                return this
            }
            val nodeToMoveOnto = managedIndexMetaData.actionMetaData.actionProperties.shrinkActionProperties.nodeName
            var numShardsOnNode = 0
            val shardToCheckpointSetMap: HashMap<ShardId, HashSet<Long>> = HashMap()
            for (shard: ShardStats in response.shards) {
                val checkpoint = shard.seqNoStats!!.localCheckpoint
                val routingInfo: ShardRouting = shard.shardRouting
                val shardId = shard.shardRouting.shardId()
                if (shardId in shardToCheckpointSetMap.keys && !shardToCheckpointSetMap[shardId]!!.equals(checkpoint)) {
                    shardToCheckpointSetMap[shardId]!!.add(checkpoint)
                    val checkPointSet = shardToCheckpointSetMap[shardId]
                    logger.warn("There are shards with varying local checkpoints for $shardId. The checkpoints are $checkPointSet.")
                } else {
                    shardToCheckpointSetMap[shardId] = HashSet()
                    shardToCheckpointSetMap[shardId]!!.add(checkpoint)
                }

                val nodeIdShardIsOn = routingInfo.currentNodeId()
                val nodeShardIsOn = clusterService.state().nodes()[nodeIdShardIsOn].name
                if (nodeShardIsOn.equals(nodeToMoveOnto) && routingInfo.started()) {
                    numShardsOnNode++
                }
            }
            if (numShardsOnNode >= numPrimaryShards) {
                info = mapOf("message" to getSuccessMessage(managedIndexMetaData.index, nodeToMoveOnto))
                stepStatus = StepStatus.COMPLETED
                return this
            }
            val numShardsLeft = numPrimaryShards - numShardsOnNode
            checkTimeOut(numShardsLeft, nodeToMoveOnto)
            return this
        } catch (e: RemoteTransportException) {
            releaseShrinkLock(managedIndexMetaData, context, logger)
            info = mapOf("message" to getFailureMessage(indexName))
            stepStatus = StepStatus.FAILED
            return this
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        // Saving maxNumSegments in ActionProperties after the force merge operation has begun so that if a ChangePolicy occurred
        // in between this step and WaitForForceMergeStep, a cached segment count expected from the operation is available
        val currentActionMetaData = currentMetaData.actionMetaData
        return currentMetaData.copy(
            actionMetaData = currentActionMetaData?.copy(),
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    private suspend fun checkTimeOut(numShardsLeft: Int, nodeToMoveOnto: String) {
        val timeFromActionStarted: Duration = Duration.between(getActionStartTime(managedIndexMetaData), Instant.now())
        val timeOutInSeconds = config.configTimeout?.timeout?.seconds ?: MOVE_SHARDS_TIMEOUT_IN_SECONDS
        // Get ActionTimeout if given, otherwise use default timeout of 12 hours
        stepStatus = if (timeFromActionStarted.toSeconds() > timeOutInSeconds) {
            logger.error(
                "The shards of "
            )
            releaseShrinkLock(managedIndexMetaData, context, logger)
            info = mapOf("message" to getTimeoutFailure(managedIndexMetaData.index, nodeToMoveOnto))
            StepStatus.FAILED
        } else {
            logger.debug(
                "Move shards still running on [$indexName] with" +
                    " [$numShardsLeft] shards still needing to be moved"
            )
            info = mapOf("message" to getTimeoutDelay(managedIndexMetaData.index, nodeToMoveOnto))
            StepStatus.CONDITION_NOT_MET
        }
    }

    companion object {
        const val name = "wait_for_move_shards_step"
        fun getSuccessMessage(index: String, node: String) = "The shards of $index successfully moved to $node."
        fun getTimeoutFailure(index: String, node: String) = "Shrink failed because it took to long to move $index shards to $node"
        fun getTimeoutDelay(index: String, node: String) = "Shrink delayed because it took to long to move $index shards to $node"
        fun getFailureMessage(index: String) = "Shrink failed on $index when waiting for shards to move."
        const val MOVE_SHARDS_TIMEOUT_IN_SECONDS = 43200L // 12hrs in seconds
        const val RESOURCE_NAME = "node_name"
        const val RESOURCE_TYPE = "shrink"
    }
}
