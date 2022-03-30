/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.issueUpdateSettingsRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
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

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount", "LongMethod")
    override suspend fun execute(): WaitForShrinkStep {
        val context = this.context ?: return this
        val actionMetadata = context.metadata.actionMetaData
        val shrinkActionProperties = actionMetadata?.actionProperties?.shrinkActionProperties
        if (shrinkActionProperties == null) {
            info = mapOf("message" to "Shrink action properties are null, metadata was not properly populated")
            stepStatus = StepStatus.FAILED
            return this
        }
        try {
            val targetIndex = shrinkActionProperties.targetIndexName
            val numPrimaryShardsStarted = getNumPrimaryShardsStarted(context.client, targetIndex)
            val numPrimaryShards = context.clusterService.state().metadata.indices[targetIndex].numberOfShards
            if (numPrimaryShards != shrinkActionProperties.targetNumShards || numPrimaryShardsStarted != shrinkActionProperties.targetNumShards) {
                checkTimeOut(context, shrinkActionProperties, targetIndex)
                return this
            }

            // Clear source and target allocation, if either fails the step will be set to failed and the function will return false
            if (!clearAllocationSettings(context, targetIndex, shrinkActionProperties)) return this
            if (!clearAllocationSettings(context, context.metadata.index, shrinkActionProperties)) return this

            releaseShrinkLock(shrinkActionProperties, context.jobContext, logger)
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to SUCCESS_MESSAGE)
            return this
        } catch (e: RemoteTransportException) {
            releaseShrinkLock(shrinkActionProperties, context.jobContext, logger)
            info = mapOf("message" to getFailureMessage(shrinkActionProperties.targetIndexName))
            stepStatus = StepStatus.FAILED
            return this
        } catch (e: Exception) {
            releaseShrinkLock(shrinkActionProperties, context.jobContext, logger)
            info = mapOf("message" to GENERIC_FAILURE_MESSAGE, "cause" to "{${e.message}}")
            stepStatus = StepStatus.FAILED
            return this
        }
    }

    private suspend fun clearAllocationSettings(context: StepContext, index: String, shrinkActionProperties: ShrinkActionProperties): Boolean {
        val allocationSettings = Settings.builder().putNull(AttemptMoveShardsStep.ROUTING_SETTING).build()
        val response: AcknowledgedResponse = issueUpdateSettingsRequest(context.client, index, allocationSettings)
        if (!response.isAcknowledged) {
            releaseShrinkLock(shrinkActionProperties, context.jobContext, logger)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailureMessage(index))
            return false
        }
        return true
    }

    private suspend fun getNumPrimaryShardsStarted(client: Client, targetIndex: String): Int {
        val targetIndexStatsRequests: IndicesStatsRequest = IndicesStatsRequest().indices(targetIndex)
        val targetStatsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(targetIndexStatsRequests, it) }
        return targetStatsResponse.shards.filter { it.shardRouting.started() && it.shardRouting.primary() }.size
    }

    private suspend fun checkTimeOut(stepContext: StepContext, shrinkActionProperties: ShrinkActionProperties, targetIndex: String) {
        val managedIndexMetadata = stepContext.metadata
        val indexName = managedIndexMetadata.index
        val timeFromActionStarted: Duration = Duration.between(getActionStartTime(managedIndexMetadata), Instant.now())
        val timeOutInSeconds = action.configTimeout?.timeout?.seconds ?: WaitForMoveShardsStep.MOVE_SHARDS_TIMEOUT_IN_SECONDS
        // Get ActionTimeout if given, otherwise use default timeout of 12 hours
        stepStatus = if (timeFromActionStarted.toSeconds() > timeOutInSeconds) {
            logger.error(
                "Shards of $indexName have still not started."
            )
            releaseShrinkLock(shrinkActionProperties, stepContext.jobContext, logger)
            info = mapOf("message" to getFailureMessage(targetIndex))
            StepStatus.FAILED
        } else {
            logger.debug(
                "Shards of $indexName have still not started."
            )
            info = mapOf("message" to getDelayedMessage(targetIndex))
            StepStatus.CONDITION_NOT_MET
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

    override fun isIdempotent() = true

    companion object {
        const val name = "wait_for_shrink_step"
        const val SUCCESS_MESSAGE = "Shrink finished successfully."
        const val GENERIC_FAILURE_MESSAGE = "Shrink failed while waiting for shards to start."
        fun getDelayedMessage(newIndex: String) = "Shrink delayed because $newIndex shards not in started state."
        fun getFailureMessage(newIndex: String) = "Shrink failed while waiting for $newIndex shards to start."
    }
}
