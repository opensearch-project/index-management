/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.indexmanagement.indexstatemanagement.action.ForceMergeAction
import org.opensearch.indexmanagement.opensearchapi.getUsefulCauseString
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.rest.RestStatus
import java.time.Duration
import java.time.Instant

class WaitForForceMergeStep(private val action: ForceMergeAction) : Step(name, false) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught", "ReturnCount")
    override suspend fun execute(): WaitForForceMergeStep {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        // Retrieve maxNumSegments value from ActionProperties. If ActionProperties is null, update failed info and return early.
        val maxNumSegments = getMaxNumSegments(context) ?: return this

        // Get the number of shards with a segment count greater than maxNumSegments, meaning they are still merging
        val shardsStillMergingSegments = getShardsStillMergingSegments(indexName, maxNumSegments, context)
        // If shardsStillMergingSegments is null, failed info has already been updated and can return early
        shardsStillMergingSegments ?: return this

        // If there are no longer shardsStillMergingSegments, then the force merge has completed
        if (shardsStillMergingSegments == 0) {
            val message = getSuccessMessage(indexName)
            logger.info(message)
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to message)
        } else {
            /*
             * If there are still shards with segments merging then no action is taken and the step will be reevaluated
             * on the next run of the ManagedIndexRunner.
             *
             * However, if a given ActionTimeout or an internal timeout of 12 hours is reached, the force_merge action
             * will be marked as failed. The internal timeout is for cases where the force merge stops silently,
             * such as when shards relocate to different nodes during a force merge operation. Since ActionTimeout
             * is optional, if no timeout is given, the segment count would stop going down as merging would no longer
             * occur and the managed index would become stuck in this action.
             */
            val timeWaitingForForceMerge: Duration = Duration.between(getActionStartTime(context), Instant.now())
            // Get ActionTimeout if given, otherwise use default timeout of 12 hours
            val timeoutInSeconds: Long = action.configTimeout?.timeout?.seconds ?: FORCE_MERGE_TIMEOUT_IN_SECONDS

            if (timeWaitingForForceMerge.seconds > timeoutInSeconds) {
                logger.error(
                    "Force merge on [$indexName] timed out with" +
                        " [$shardsStillMergingSegments] shards containing unmerged segments"
                )

                stepStatus = StepStatus.FAILED
                info = mapOf("message" to getFailedTimedOutMessage(indexName))
            } else {
                logger.debug(
                    "Force merge still running on [$indexName] with" +
                        " [$shardsStillMergingSegments] shards containing unmerged segments"
                )

                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getWaitingMessage(indexName))
            }
        }

        return this
    }

    private fun getMaxNumSegments(context: StepContext): Int? {
        val managedIndexMetaData = context.metadata
        val actionProperties = managedIndexMetaData.actionMetaData?.actionProperties

        if (actionProperties?.maxNumSegments == null) {
            stepStatus = StepStatus.FAILED
            info = mapOf(
                "message" to "Unable to retrieve [${ActionProperties.Properties.MAX_NUM_SEGMENTS.key}]" +
                    " from ActionProperties=$actionProperties"
            )
            return null
        }

        return actionProperties.maxNumSegments
    }

    private suspend fun getShardsStillMergingSegments(indexName: String, maxNumSegments: Int, context: StepContext): Int? {
        try {
            val statsRequest = IndicesStatsRequest().indices(indexName)
            val statsResponse: IndicesStatsResponse = context.client.admin().indices().suspendUntil { stats(statsRequest, it) }

            if (statsResponse.status == RestStatus.OK) {
                return statsResponse.shards.count {
                    val count = it.stats.segments?.count
                    if (count == null) {
                        logger.warn("$indexName wait for force merge had null segments")
                        false
                    } else {
                        count > maxNumSegments
                    }
                }
            }

            val message = getFailedSegmentCheckMessage(indexName)
            logger.warn("$message - ${statsResponse.status}")
            stepStatus = StepStatus.FAILED
            info = mapOf(
                "message" to message,
                "shard_failures" to statsResponse.shardFailures.map { it.getUsefulCauseString() }
            )
        } catch (e: Exception) {
            val message = getFailedSegmentCheckMessage(indexName)
            logger.error(message, e)
            stepStatus = StepStatus.FAILED
            val mutableInfo = mutableMapOf("message" to message)
            val errorMessage = e.message
            if (errorMessage != null) mutableInfo["cause"] = errorMessage
            info = mutableInfo.toMap()
        }

        return null
    }

    private fun getActionStartTime(context: StepContext): Instant {
        val managedIndexMetaData = context.metadata
        val startTime = managedIndexMetaData.actionMetaData?.startTime ?: return Instant.now()

        return Instant.ofEpochMilli(startTime)
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        // if the step is completed set actionProperties back to null
        val currentActionMetaData = currentMetadata.actionMetaData
        val updatedActionMetaData = currentActionMetaData?.let {
            if (stepStatus != StepStatus.COMPLETED) it
            else currentActionMetaData.copy(actionProperties = null)
        }
        return currentMetadata.copy(
            actionMetaData = updatedActionMetaData,
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent() = true

    companion object {
        const val name = "wait_for_force_merge"
        const val FORCE_MERGE_TIMEOUT_IN_SECONDS = 43200L // 12 hours
        fun getFailedTimedOutMessage(index: String) = "Force merge timed out [index=$index]"
        fun getFailedSegmentCheckMessage(index: String) = "Failed to check segments when waiting for force merge to complete [index=$index]"
        fun getWaitingMessage(index: String) = "Waiting for force merge to complete [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully confirmed segments force merged [index=$index]"
    }
}
