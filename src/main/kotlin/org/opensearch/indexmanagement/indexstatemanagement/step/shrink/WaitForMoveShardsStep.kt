/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.getSecurityFailureMessage
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.renewShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.getUpdatedShrinkActionProperties
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException
import java.lang.Exception
import java.time.Duration
import java.time.Instant

class WaitForMoveShardsStep(private val action: ShrinkAction) : ShrinkStep(name) {

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount", "NestedBlockDepth")
    override suspend fun execute(): WaitForMoveShardsStep {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val actionMetadata = context.metadata.actionMetaData
        val localShrinkActionProperties = actionMetadata?.actionProperties?.shrinkActionProperties
        shrinkActionProperties = localShrinkActionProperties
        if (localShrinkActionProperties == null) {
            cleanupAndFail(METADATA_FAILURE_MESSAGE, METADATA_FAILURE_MESSAGE)
            return this
        }
        val lock = renewShrinkLock(localShrinkActionProperties, context.lockService, logger)
        if (lock == null) {
            cleanupAndFail(
                "Failed to renew lock on node [${localShrinkActionProperties.nodeName}]",
                "Shrink action failed to renew lock on node [${localShrinkActionProperties.nodeName}]"
            )
            return this
        }
        // After renewing the lock we need to update the primary term and sequence number
        shrinkActionProperties = getUpdatedShrinkActionProperties(localShrinkActionProperties, lock)
        try {
            val indexStatsRequests: IndicesStatsRequest = IndicesStatsRequest().indices(indexName)
            val response: IndicesStatsResponse = context.client.admin().indices().suspendUntil { stats(indexStatsRequests, it) }
            val numPrimaryShards = context.clusterService.state().metadata.indices[indexName].numberOfShards
            val nodeToMoveOnto = localShrinkActionProperties.nodeName
            val inSyncAllocations = context.clusterService.state().metadata.indices[indexName].inSyncAllocationIds
            val numReplicas = context.clusterService.state().metadata.indices[indexName].numberOfReplicas
            val shardIdOnNode: MutableMap<Int, Boolean> = mutableMapOf()
            var numShardsInSync = 0
            for (shard: ShardStats in response.shards) {
                val routingInfo = shard.shardRouting
                val nodeIdShardIsOn = routingInfo.currentNodeId()
                val nodeNameShardIsOn = context.clusterService.state().nodes()[nodeIdShardIsOn].name
                if (nodeNameShardIsOn.equals(nodeToMoveOnto) && routingInfo.started()) {
                    shardIdOnNode[shard.shardRouting.id] = true
                }
                if (routingInfo.primary()) {
                    // Either there must be no replicas (force unsafe must have been set) or all replicas must be in sync as
                    // it isn't known which shard (any replica or primary) will be moved to the target node and used in the shrink.
                    if (numReplicas == 0 || inSyncAllocations[routingInfo.id].size == (numReplicas + 1)) {
                        numShardsInSync++
                    }
                }
            }
            if (shardIdOnNode.values.all { it } && numShardsInSync >= numPrimaryShards) {
                info = mapOf("message" to getSuccessMessage(nodeToMoveOnto))
                stepStatus = StepStatus.COMPLETED
            } else {
                val numShardsNotOnNode = shardIdOnNode.values.count { !it }
                val numShardsNotInSync = numPrimaryShards - numShardsInSync
                checkTimeOut(context, numShardsNotOnNode, numShardsNotInSync, nodeToMoveOnto)
            }
            return this
        } catch (e: OpenSearchSecurityException) {
            val securityFailureMessage = getSecurityFailureMessage(e.localizedMessage)
            cleanupAndFail(securityFailureMessage, securityFailureMessage, e.message, e)
            return this
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e)
            cleanupAndFail(FAILURE_MESSAGE, FAILURE_MESSAGE, cause = e.message, e = unwrappedException as Exception)
            return this
        } catch (e: Exception) {
            cleanupAndFail(FAILURE_MESSAGE, FAILURE_MESSAGE, cause = e.message, e)
            return this
        }
    }

    // Sets the action to failed, clears the readonly and allocation settings on the source index, and releases the shrink lock
    private suspend fun cleanupAndFail(infoMessage: String, logMessage: String? = null, cause: String? = null, e: Exception? = null) {
        cleanupResources(resetSettings = true, releaseLock = true, deleteTargetIndex = false)
        fail(infoMessage, logMessage, cause, e)
    }

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
