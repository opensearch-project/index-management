/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.rollup

import org.apache.logging.log4j.LogManager
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupRequest
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupResponse
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.transport.RemoteTransportException
import java.lang.Exception

class WaitForRollupCompletionStep(
    val clusterService: ClusterService,
    val client: Client,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private val logger = LogManager.getLogger(javaClass)
    private var hasRollupFailed: Boolean? = null

    override fun isIdempotent() = true

    override suspend fun execute(): WaitForRollupCompletionStep {
        val rollupJobId = managedIndexMetaData.actionMetaData?.actionProperties?.rollupId

        if (rollupJobId == null) {
            logger.error("No rollup job id passed down")
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getMissingRollupJobMessage(indexName))
        } else {
            val explainRollupRequest = ExplainRollupRequest(listOf(rollupJobId))
            try {
                val response: ExplainRollupResponse = client.suspendUntil { execute(ExplainRollupAction.INSTANCE, explainRollupRequest, it) }
                logger.info("Received the status for jobs [${response.getIdsToExplain().keys}]")
                val metadata = response.getIdsToExplain()[rollupJobId]?.metadata

                if (metadata?.status == null) {
                    logger.warn("Job $rollupJobId has not started yet")
                    stepStatus = StepStatus.CONDITION_NOT_MET
                    info = mapOf("message" to getJobProcessingMessage(rollupJobId, indexName))
                } else {
                    processRollupMetadataStatus(rollupJobId, metadata)
                }
            } catch (e: RemoteTransportException) {
                processFailure(rollupJobId, e)
            } catch (e: Exception) {
                processFailure(rollupJobId, e)
            }
        }

        return this
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetaData.actionMetaData
        val currentActionProperties = currentActionMetaData?.actionProperties
        return currentMetaData.copy(
            actionMetaData = currentActionMetaData?.copy(
                actionProperties = currentActionProperties?.copy(
                    hasRollupFailed = hasRollupFailed
                )
            ),
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    fun processRollupMetadataStatus(rollupJobId: String, rollupMetadata: RollupMetadata) {
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
                info = mapOf("message" to getJobFailedMessage(rollupJobId, indexName), "cause" to getJobStoppedMessage())
            }
        }
    }

    fun processFailure(rollupJobId: String, e: Exception) {
        stepStatus = StepStatus.FAILED
        val message = getFailedMessage(rollupJobId, indexName)
        logger.error(message, e)
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    companion object {
        const val name = "wait_for_rollup_completion"
        fun getFailedMessage(rollupJob: String, index: String) = "Failed to get the status of rollup job [$rollupJob] [index=$index]"
        fun getJobProcessingMessage(rollupJob: String, index: String) = "Rollup job [$rollupJob] is still processing [index=$index]"
        fun getJobCompletionMessage(rollupJob: String, index: String) = "Rollup job [$rollupJob] completed [index=$index]"
        fun getJobFailedMessage(rollupJob: String, index: String) = "Rollup job [$rollupJob] failed [index=$index]"
        fun getJobStoppedMessage() = "Rollup job was stopped"
        fun getMissingRollupJobMessage(index: String) = "Rollup job was not found [index=$index]"
    }
}
