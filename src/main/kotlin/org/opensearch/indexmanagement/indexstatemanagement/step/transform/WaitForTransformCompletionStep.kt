/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.transform

import org.apache.logging.log4j.LogManager
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.indexmanagement.transform.action.explain.ExplainTransformAction
import org.opensearch.indexmanagement.transform.action.explain.ExplainTransformRequest
import org.opensearch.indexmanagement.transform.action.explain.ExplainTransformResponse
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.transport.RemoteTransportException

class WaitForTransformCompletionStep : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var hasTransformFailed: Boolean? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val managedIndexMetadata = context.metadata
        val transformJobId = managedIndexMetadata.actionMetaData?.actionProperties?.transformActionProperties?.transformId

        if (transformJobId == null) {
            logger.error("No transform job id passed down.")
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getMissingTransformJobMessage(indexName))
        } else {
            val explainTransformRequest = ExplainTransformRequest(listOf(transformJobId))
            try {
                val response: ExplainTransformResponse = context.client.suspendUntil { execute(ExplainTransformAction.INSTANCE, explainTransformRequest, it) }
                logger.info("Received the status for jobs [${response.getIdsToExplain().keys}]")
                val metadata = response.getIdsToExplain()[transformJobId]?.metadata

                if (metadata?.status == null) {
                    logger.warn("Job $transformJobId has not started yet")
                    stepStatus = StepStatus.CONDITION_NOT_MET
                    info = mapOf("message" to getJobProcessingMessage(transformJobId, indexName))
                } else {
                    processTransformMetadataStatus(transformJobId, indexName, metadata)
                }
            } catch (e: RemoteTransportException) {
                processFailure(transformJobId, indexName, e)
            } catch (e: Exception) {
                processFailure(transformJobId, indexName, e)
            }
        }

        return this
    }

    fun processTransformMetadataStatus(transformJobId: String, indexName: String, transformMetadata: TransformMetadata) {
        when (transformMetadata.status) {
            TransformMetadata.Status.INIT, TransformMetadata.Status.STARTED -> {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getJobProcessingMessage(transformJobId, indexName))
            }
            TransformMetadata.Status.FAILED -> {
                stepStatus = StepStatus.FAILED
                hasTransformFailed = true
                info = mapOf("message" to getJobFailedMessage(transformJobId, indexName), "cause" to "${transformMetadata.failureReason}")
            }
            TransformMetadata.Status.FINISHED -> {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getJobCompletionMessage(transformJobId, indexName))
            }
            TransformMetadata.Status.STOPPED -> {
                stepStatus = StepStatus.FAILED
                hasTransformFailed = true
                info = mapOf("message" to getJobFailedMessage(transformJobId, indexName), "cause" to JOB_STOPPED_MESSAGE)
            }
        }
    }

    fun processFailure(transformJobId: String, indexName: String, e: Exception) {
        stepStatus = StepStatus.FAILED
        val message = getFailedMessage(transformJobId, indexName)
        logger.error(message, e)
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetadata = currentMetadata.actionMetaData
        val currentActionProperties = currentActionMetadata?.actionProperties
        val currentTransformActionProperties = currentActionProperties?.transformActionProperties
        return currentMetadata.copy(
            actionMetaData = currentActionMetadata?.copy(
                actionProperties = currentActionProperties?.copy(
                    transformActionProperties = currentTransformActionProperties?.copy(
                        hasTransformFailed = hasTransformFailed
                    )
                )
            ),
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent(): Boolean = true

    companion object {
        const val name = "wait_for_transform_completion"
        const val JOB_STOPPED_MESSAGE = "Transform job was stopped"
        fun getFailedMessage(transformJob: String, index: String) = "Failed to get the status of transform job [$transformJob] [index=$index]"
        fun getJobProcessingMessage(transformJob: String, index: String) = "Transform job [$transformJob] is still processing [index=$index]"
        fun getJobCompletionMessage(transformJob: String, index: String) = "Transform job [$transformJob] completed [index=$index]"
        fun getJobFailedMessage(transformJob: String, index: String) = "Transform job [$transformJob] failed [index=$index]"
        fun getMissingTransformJobMessage(index: String) = "Transform job was not found [index=$index]"
    }
}
