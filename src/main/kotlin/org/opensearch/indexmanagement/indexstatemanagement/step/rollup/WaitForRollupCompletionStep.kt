/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.rollup

import org.apache.logging.log4j.LogManager
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupRequest
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupResponse
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException

class WaitForRollupCompletionStep : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var hasRollupFailed: Boolean? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val managedIndexMetadata = context.metadata
        val rollupJobId = managedIndexMetadata.actionMetaData?.actionProperties?.rollupId

        if (rollupJobId == null) {
            logger.error("No rollup job id passed down")
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getMissingRollupJobMessage(indexName))
        } else {
            val explainRollupRequest = ExplainRollupRequest(listOf(rollupJobId))
            try {
                val response: ExplainRollupResponse = context.client.suspendUntil { execute(ExplainRollupAction.INSTANCE, explainRollupRequest, it) }
                logger.info("Received the status for jobs [${response.getIdsToExplain().keys}]")
                val metadata = response.getIdsToExplain()[rollupJobId]?.metadata

                if (metadata?.status == null) {
                    logger.warn("Job $rollupJobId has not started yet")
                    stepStatus = StepStatus.CONDITION_NOT_MET
                    info = mapOf("message" to getJobProcessingMessage(rollupJobId, indexName))
                } else {
                    processRollupMetadataStatus(rollupJobId, indexName, metadata)
                }
            } catch (e: RemoteTransportException) {
                processFailure(rollupJobId, indexName, e)
            } catch (e: Exception) {
                processFailure(rollupJobId, indexName, e)
            }
        }

        return this
    }

    fun processRollupMetadataStatus(rollupJobId: String, indexName: String, rollupMetadata: RollupMetadata) {
        when (rollupMetadata.status) {
            RollupMetadata.Status.INIT -> {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getJobProcessingMessage(rollupJobId, indexName))
            }
            RollupMetadata.Status.STARTED -> {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getJobProcessingMessage(rollupJobId, indexName))
            }
            RollupMetadata.Status.FAILED -> {
                stepStatus = StepStatus.FAILED
                hasRollupFailed = true
                info = mapOf("message" to getJobFailedMessage(rollupJobId, indexName), "cause" to "${rollupMetadata.failureReason}")
            }
            RollupMetadata.Status.FINISHED -> {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getJobCompletionMessage(rollupJobId, indexName))
            }
            RollupMetadata.Status.RETRY -> {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getJobProcessingMessage(rollupJobId, indexName))
            }
            RollupMetadata.Status.STOPPED -> {
                stepStatus = StepStatus.FAILED
                hasRollupFailed = true
                info = mapOf("message" to getJobFailedMessage(rollupJobId, indexName), "cause" to JOB_STOPPED_MESSAGE)
            }
        }
    }

    fun processFailure(rollupJobId: String, indexName: String, e: Exception) {
        stepStatus = StepStatus.FAILED
        val message = getFailedMessage(rollupJobId, indexName)
        logger.error(message, e)
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetadata.actionMetaData
        val currentActionProperties = currentActionMetaData?.actionProperties
        return currentMetadata.copy(
            actionMetaData = currentActionMetaData?.copy(
                actionProperties = currentActionProperties?.copy(
                    hasRollupFailed = hasRollupFailed
                )
            ),
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent(): Boolean = true

    companion object {
        const val name = "wait_for_rollup_completion"
        const val JOB_STOPPED_MESSAGE = "Rollup job was stopped"
        fun getFailedMessage(rollupJob: String, index: String) = "Failed to get the status of rollup job [$rollupJob] [index=$index]"
        fun getJobProcessingMessage(rollupJob: String, index: String) = "Rollup job [$rollupJob] is still processing [index=$index]"
        fun getJobCompletionMessage(rollupJob: String, index: String) = "Rollup job [$rollupJob] completed [index=$index]"
        fun getJobFailedMessage(rollupJob: String, index: String) = "Rollup job [$rollupJob] failed [index=$index]"
        fun getMissingRollupJobMessage(index: String) = "Rollup job was not found [index=$index]"
    }
}
