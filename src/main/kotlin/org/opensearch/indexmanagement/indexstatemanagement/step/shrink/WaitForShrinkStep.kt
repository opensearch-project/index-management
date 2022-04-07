/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.util.clearReadOnlyAndRouting
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.issueUpdateSettingsRequest
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
import java.time.Duration
import java.time.Instant

class WaitForShrinkStep(private val action: ShrinkAction) : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var shrinkActionProperties: ShrinkActionProperties? = null

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount", "LongMethod")
    override suspend fun execute(): WaitForShrinkStep {
        val context = this.context ?: return this
        val actionMetadata = context.metadata.actionMetaData
        val localShrinkActionProperties = actionMetadata?.actionProperties?.shrinkActionProperties
        shrinkActionProperties = localShrinkActionProperties
        if (localShrinkActionProperties == null) {
            cleanupAndFail(WaitForMoveShardsStep.METADATA_FAILURE_MESSAGE)
            return this
        }
        val lock = renewShrinkLock(localShrinkActionProperties, context.jobContext, logger)
        if (lock == null) {
            cleanupAndFail("Failed to renew lock on node [${localShrinkActionProperties.nodeName}]")
            return this
        }
        shrinkActionProperties = getUpdatedShrinkActionProperties(localShrinkActionProperties, lock)
        try {
            val targetIndex = localShrinkActionProperties.targetIndexName
            val numPrimaryShardsStarted = getNumPrimaryShardsStarted(context.client, targetIndex)
            val numPrimaryShards = context.clusterService.state().metadata.indices[targetIndex].numberOfShards
            val targetNumShards = localShrinkActionProperties.targetNumShards
            if (numPrimaryShards != targetNumShards || numPrimaryShardsStarted != targetNumShards) {
                checkTimeOut(context, targetIndex)
                return this
            }

            // Clear source and target allocation, if either fails the step will be set to failed and the function will return false
            if (!clearAllocationSettings(context, targetIndex)) return this
            if (!clearAllocationSettings(context, context.metadata.index)) return this

            deleteShrinkLock(localShrinkActionProperties, context.jobContext, logger)
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to SUCCESS_MESSAGE)
            return this
        } catch (e: RemoteTransportException) {
            cleanupAndFail(getFailureMessage(localShrinkActionProperties.targetIndexName))
            return this
        } catch (e: Exception) {
            cleanupAndFail(GENERIC_FAILURE_MESSAGE, e.message)
            return this
        }
    }

    // Sets the action to failed, clears the readonly and allocation settings on the source index, deletes the target index,
    // and releases the shrink lock
    private suspend fun cleanupAndFail(message: String, cause: String? = null) {
        info = if (cause == null) mapOf("message" to message) else mapOf("message" to message, "cause" to cause)
        stepStatus = StepStatus.FAILED
        // Using a try/catch for each cleanup action as we should clean up as much as possible despite any failures
        // Non-null assertion !! is used to throw an exception on null which would just be caught and logged
        try {
            clearReadOnlyAndRouting(context!!.metadata.index, context!!.client)
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to clean up routing and readonly setting after a failure: $e")
        }
        try {
            // Use plugin level permissions when deleting the failed target shrink index after a failure
            context!!.client.threadPool().threadContext.stashContext().use {
                val deleteRequest = DeleteIndexRequest(shrinkActionProperties!!.targetIndexName)
                val response: AcknowledgedResponse =
                    context!!.client.admin().indices().suspendUntil { delete(deleteRequest, it) }
                if (!response.isAcknowledged) {
                    logger.error("Shrink action failed to delete target index during cleanup after a failure")
                }
            }
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to delete the target index after a failure: $e")
        }
        try {
            releaseShrinkLock(shrinkActionProperties!!, context!!.jobContext, logger)
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to release the node lock after a failure: $e")
        }
        shrinkActionProperties = null
    }

    private suspend fun clearAllocationSettings(context: StepContext, index: String): Boolean {
        val allocationSettings = Settings.builder().putNull(AttemptMoveShardsStep.ROUTING_SETTING).build()
        val response: AcknowledgedResponse = issueUpdateSettingsRequest(context.client, index, allocationSettings)
        if (!response.isAcknowledged) {
            cleanupAndFail(getFailureMessage(index))
            return false
        }
        return true
    }

    private suspend fun getNumPrimaryShardsStarted(client: Client, targetIndex: String): Int {
        val targetIndexStatsRequests: IndicesStatsRequest = IndicesStatsRequest().indices(targetIndex)
        val targetStatsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(targetIndexStatsRequests, it) }
        return targetStatsResponse.shards.filter { it.shardRouting.started() && it.shardRouting.primary() }.size
    }

    private suspend fun checkTimeOut(stepContext: StepContext, targetIndex: String) {
        val managedIndexMetadata = stepContext.metadata
        val timeFromActionStarted: Duration = Duration.between(getActionStartTime(managedIndexMetadata), Instant.now())
        val timeOutInSeconds = action.configTimeout?.timeout?.seconds ?: WaitForMoveShardsStep.MOVE_SHARDS_TIMEOUT_IN_SECONDS
        // Get ActionTimeout if given, otherwise use default timeout of 12 hours
        if (timeFromActionStarted.toSeconds() > timeOutInSeconds) {
            cleanupAndFail(getFailureMessage(targetIndex))
        } else {
            info = mapOf("message" to getDelayedMessage(targetIndex))
            stepStatus = StepStatus.CONDITION_NOT_MET
        }
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

    override fun isIdempotent() = true

    companion object {
        const val name = "wait_for_shrink_step"
        const val SUCCESS_MESSAGE = "Shrink finished successfully."
        const val GENERIC_FAILURE_MESSAGE = "Shrink failed while waiting for shards to start."
        fun getDelayedMessage(newIndex: String) = "Shrink delayed because $newIndex shards not in started state."
        fun getFailureMessage(newIndex: String) = "Shrink failed while waiting for $newIndex shards to start."
    }
}
