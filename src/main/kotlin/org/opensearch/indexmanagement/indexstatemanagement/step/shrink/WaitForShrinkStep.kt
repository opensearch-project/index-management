/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.issueUpdateSettingsRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.resetReadOnlyAndRouting
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import java.time.Duration
import java.time.Instant

class WaitForShrinkStep(private val action: ShrinkAction) : ShrinkStep(name, true, true, true) {

    @Suppress("ReturnCount")
    override suspend fun wrappedExecute(context: StepContext): WaitForShrinkStep {
        val indexName = context.metadata.index
        // If the returned shrinkActionProperties are null, then the status has been set to failed, just return
        val localShrinkActionProperties = checkShrinkActionPropertiesAndRenewLock(context) ?: return this

        val targetIndex = localShrinkActionProperties.targetIndexName
        if (shrinkNotDone(targetIndex, localShrinkActionProperties.targetNumShards, context.client, context.clusterService)) {
            checkTimeOut(context, targetIndex)
            return this
        }

        // Clear source and target allocation, if either fails the step will be set to failed and the function will return false
        if (!clearAllocationSettings(context, targetIndex)) return this
        if (!resetReadOnlyAndRouting(indexName, context.client, localShrinkActionProperties.originalIndexSettings)) return this

        if (!deleteShrinkLock(localShrinkActionProperties, context.lockService, logger)) {
            logger.error("Failed to delete Shrink action lock on node [${localShrinkActionProperties.nodeName}]")
        }

        if (switchAliases(context, localShrinkActionProperties)) {
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to SUCCESS_MESSAGE)
        } else {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to "Shrink failed due to aliases switch failure.")
        }

        return this
    }

    override fun getGenericFailureMessage(): String = GENERIC_FAILURE_MESSAGE

    private suspend fun shrinkNotDone(targetIndex: String, targetNumShards: Int, client: Client, clusterService: ClusterService): Boolean {
        val numPrimaryShardsStarted = getNumPrimaryShardsStarted(client, targetIndex)
        val numPrimaryShards = clusterService.state().metadata.indices[targetIndex]?.numberOfShards
        return numPrimaryShards != targetNumShards || numPrimaryShardsStarted != targetNumShards
    }

    private suspend fun clearAllocationSettings(context: StepContext, index: String): Boolean {
        val allocationSettings = Settings.builder().putNull(AttemptMoveShardsStep.ROUTING_SETTING).build()
        val response: AcknowledgedResponse = issueUpdateSettingsRequest(context.client, index, allocationSettings)
        if (!response.isAcknowledged) {
            cleanupAndFail(
                getFailureMessage(index),
                "Shrink action to clear the allocation settings on index [$index] following shrinking."
            )
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
            val timeoutFailure = getTimeoutFailure(targetIndex)
            cleanupAndFail(timeoutFailure, timeoutFailure)
        } else {
            info = mapOf("message" to getDelayedMessage(targetIndex))
            stepStatus = StepStatus.CONDITION_NOT_MET
        }
    }

    private suspend fun switchAliases(context: StepContext, shrinkActionProperties: ShrinkActionProperties): Boolean {

        val sourceIndexName = context.metadata.index
        val targetIndexName = shrinkActionProperties.targetIndexName

        if (!action.switchAliases) {
            logger.info("Switch aliases disabled from [$sourceIndexName] to [$targetIndexName].")
            return true
        }

        logger.info("Switching aliases from [$sourceIndexName] to [$targetIndexName].")

        val targetIndexAliasesNames = context
            .clusterService
            .state()
            .metadata()
            .index(targetIndexName)
            .aliases
            .keys
        val sourceIndexAliases = context
            .clusterService
            .state()
            .metadata()
            .index(sourceIndexName)
            .aliases
            .values

        val req = IndicesAliasesRequest()
        sourceIndexAliases.map { it.alias }.forEach { req.addAliasAction(AliasActions(AliasActions.Type.REMOVE).index(sourceIndexName).alias(it)) }

        sourceIndexAliases
            .filterNot { targetIndexAliasesNames.contains(it.alias) }
            .map {
                AliasActions(AliasActions.Type.ADD)
                    .index(targetIndexName)
                    .alias(it.alias)
                    .filter(it.filter?.string())
                    .indexRouting(it.indexRouting)
                    .searchRouting(it.searchRouting)
                    .isHidden(it.isHidden)
                    .writeIndex(it.writeIndex())
            }
            .forEach { req.addAliasAction(it) }

        return try {
            val response: AcknowledgedResponse = context.client.admin().indices().suspendUntil { aliases(req, it) }
            if (response.isAcknowledged) {
                logger.info("Aliases switched successfully from [$sourceIndexName] to [$targetIndexName].")
            } else {
                logger.error("Switching aliases from [$sourceIndexName] to [$targetIndexName] failed.")
            }
            response.isAcknowledged
        } catch (e: Exception) {
            logger.error("Switching aliases from [$sourceIndexName] to [$targetIndexName] failed due to exception.", e)
            false
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
