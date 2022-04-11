/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.getSecurityFailureMessage
import org.opensearch.indexmanagement.indexstatemanagement.util.clearReadOnlyAndRouting
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.renewShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.getUpdatedShrinkActionProperties
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
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
    private var shrinkActionProperties: ShrinkActionProperties? = null

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount", "NestedBlockDepth")
    override suspend fun execute(): WaitForMoveShardsStep {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val actionMetadata = context.metadata.actionMetaData
        val localShrinkActionProperties = actionMetadata?.actionProperties?.shrinkActionProperties
        shrinkActionProperties = localShrinkActionProperties
        if (localShrinkActionProperties == null) {
            logger.error(METADATA_FAILURE_MESSAGE)
            cleanupAndFail(METADATA_FAILURE_MESSAGE)
            return this
        }
        val lock = renewShrinkLock(localShrinkActionProperties, context.jobContext, logger)
        if (lock == null) {
            logger.error("Shrink action failed to renew lock on node [${localShrinkActionProperties.nodeName}]")
            cleanupAndFail("Failed to renew lock on node [${localShrinkActionProperties.nodeName}]")
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
        } catch (e: OpenSearchSecurityException) {
            cleanupAndFail(getSecurityFailureMessage(e.localizedMessage), e.message, e)
            return this
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e)
            cleanupAndFail(FAILURE_MESSAGE, cause = e.message, e = unwrappedException as Exception)
            return this
        } catch (e: Exception) {
            cleanupAndFail(FAILURE_MESSAGE, cause = e.message, e)
            return this
        }
    }

    // Sets the action to failed, clears the readonly and allocation settings on the source index, and releases the shrink lock
    private suspend fun cleanupAndFail(message: String, cause: String? = null, e: Exception? = null) {
        e?.let { logger.error(message, e) }
        info = if (cause == null) mapOf("message" to message) else mapOf("message" to message, "cause" to cause)
        stepStatus = StepStatus.FAILED
        // Non-null assertion !! is used to throw an exception on null which would just be caught and logged
        try {
            clearReadOnlyAndRouting(context!!.metadata.index, context!!.client)
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to clean up routing and readonly setting after a failure: $e")
        }
        try {
            releaseShrinkLock(shrinkActionProperties!!, context!!.jobContext, logger)
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to release the node lock after a failure: $e")
        }
        shrinkActionProperties = null
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
    }
}
