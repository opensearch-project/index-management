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
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ShrinkActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.issueUpdateSettingsRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.transport.RemoteTransportException
import java.time.Duration
import java.time.Instant

class WaitForShrinkStep(
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

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount")
    override suspend fun execute(): WaitForShrinkStep {
        try {
            val targetIndex = managedIndexMetaData.actionMetaData!!.actionProperties!!.shrinkActionProperties!!.targetIndexName!!
            val targetIndexStatsRequests: IndicesStatsRequest = IndicesStatsRequest().indices(targetIndex)
            val targetStatsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(targetIndexStatsRequests, it) }
            var numShardsStarted = 0
            for (shard: ShardStats in targetStatsResponse.shards) {
                if (shard.shardRouting.started()) {
                    numShardsStarted++
                }
            }
            if (numShardsStarted < managedIndexMetaData.actionMetaData.actionProperties!!.shrinkActionProperties!!.targetNumShards!!) {
                checkTimeOut(targetIndex)
                return this
            }
            val allocationSettings = Settings.builder().putNull(AttemptMoveShardsStep.ROUTING_SETTING).build()
            val response: AcknowledgedResponse = client.admin().indices().suspendUntil {
                updateSettings(UpdateSettingsRequest(allocationSettings, targetIndex), it)
            }
            if (!response.isAcknowledged) {
                releaseShrinkLock(managedIndexMetaData, context, logger)
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to getFailureMessage(managedIndexMetaData.index, targetIndex))
                return this
            }
            issueUpdateSettingsRequest(client, managedIndexMetaData, allocationSettings)
            releaseShrinkLock(managedIndexMetaData, context, logger)
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(managedIndexMetaData.index))
            return this
        } catch (e: RemoteTransportException) {
            releaseShrinkLock(managedIndexMetaData, context, logger)
            info = mapOf(
                "message" to getFailureMessage(
                    managedIndexMetaData.index,
                    managedIndexMetaData.actionMetaData!!.actionProperties!!.shrinkActionProperties!!.targetIndexName!!
                )
            )
            stepStatus = StepStatus.FAILED
            return this
        }
    }

    private suspend fun checkTimeOut(targetIndex: String) {
        val timeFromActionStarted: Duration = Duration.between(getActionStartTime(managedIndexMetaData), Instant.now())
        val timeOutInSeconds = config.configTimeout?.timeout?.seconds ?: WaitForMoveShardsStep.MOVE_SHARDS_TIMEOUT_IN_SECONDS
        // Get ActionTimeout if given, otherwise use default timeout of 12 hours
        stepStatus = if (timeFromActionStarted.toSeconds() > timeOutInSeconds) {
            logger.error(
                "Shards of ${managedIndexMetaData.index} have still not started."
            )
            releaseShrinkLock(managedIndexMetaData, context, logger)
            info = mapOf("message" to getFailureMessage(managedIndexMetaData.index, targetIndex))
            StepStatus.FAILED
        } else {
            logger.debug(
                "Shards of ${managedIndexMetaData.index} have still not started."
            )
            info = mapOf("message" to getDelayedMessage(managedIndexMetaData.index, targetIndex))
            StepStatus.CONDITION_NOT_MET
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

    companion object {
        const val name = "wait_for_shrink_step"
        fun getSuccessMessage(index: String) = "Shrink finished successfully on $index."
        fun getDelayedMessage(index: String, newIndex: String) = "Shrink delayed for $index because $newIndex shards not in started state."
        fun getFailureMessage(index: String, newIndex: String) = "Shrink failed for $index while waiting for $newIndex shards to start."
    }
}
