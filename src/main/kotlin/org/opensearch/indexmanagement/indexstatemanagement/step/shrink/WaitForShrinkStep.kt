/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.getSecurityFailureMessage
import org.opensearch.indexmanagement.indexstatemanagement.util.resetReadOnlyAndRouting
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.issueUpdateSettingsRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.renewShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.getUpdatedShrinkActionProperties
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException
import java.time.Duration
import java.time.Instant

class WaitForShrinkStep(private val action: ShrinkAction) : ShrinkStep(name) {

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount", "LongMethod")
    override suspend fun execute(): WaitForShrinkStep {
        val context = this.context ?: return this
        val actionMetadata = context.metadata.actionMetaData
        val localShrinkActionProperties = actionMetadata?.actionProperties?.shrinkActionProperties
        shrinkActionProperties = localShrinkActionProperties
        if (localShrinkActionProperties == null) {
            logger.error(METADATA_FAILURE_MESSAGE)
            cleanupAndFail(METADATA_FAILURE_MESSAGE)
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
            if (!resetReadOnlyAndRouting(context.metadata.index, context.client, localShrinkActionProperties.originalIndexSettings)) return this

            deleteShrinkLock(localShrinkActionProperties, context.lockService, logger)
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to SUCCESS_MESSAGE)
            return this
        } catch (e: OpenSearchSecurityException) {
            val securityFailureMessage = getSecurityFailureMessage(e.localizedMessage)
            cleanupAndFail(securityFailureMessage, securityFailureMessage, e.message, e)
            return this
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e)
            cleanupAndFail(GENERIC_FAILURE_MESSAGE, GENERIC_FAILURE_MESSAGE, cause = e.message, e = unwrappedException as java.lang.Exception)
            return this
        } catch (e: Exception) {
            cleanupAndFail(GENERIC_FAILURE_MESSAGE, GENERIC_FAILURE_MESSAGE, e.message, e)
            return this
        }
    }

    // Sets the action to failed, clears the readonly and allocation settings on the source index, deletes the target index,
    // and releases the shrink lock
    private suspend fun cleanupAndFail(infoMessage: String, logMessage: String? = null, cause: String? = null, e: Exception? = null) {
        cleanupResources(resetSettings = true, releaseLock = true, deleteTargetIndex = true)
        fail(infoMessage, logMessage, cause, e)
    }

    private suspend fun clearAllocationSettings(context: StepContext, index: String): Boolean {
        val allocationSettings = Settings.builder().putNull(AttemptMoveShardsStep.ROUTING_SETTING).build()
        val response: AcknowledgedResponse = issueUpdateSettingsRequest(context.client, index, allocationSettings)
        if (!response.isAcknowledged) {
            logger.error("Shrink action to clear the allocation settings on index [$index] following shrinking.")
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
            logger.error(getTimeoutFailure(targetIndex))
            cleanupAndFail(getTimeoutFailure(targetIndex))
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
        fun getTimeoutFailure(newIndex: String) = "Shrink failed because it timed out while waiting for $newIndex shrink to finish."
    }
}
