/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.transform

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchException
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.indexmanagement.indexstatemanagement.action.TransformAction
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.TransformActionProperties
import org.opensearch.indexmanagement.transform.action.index.IndexTransformAction
import org.opensearch.indexmanagement.transform.action.index.IndexTransformRequest
import org.opensearch.indexmanagement.transform.action.index.IndexTransformResponse
import org.opensearch.indexmanagement.transform.action.start.StartTransformAction
import org.opensearch.indexmanagement.transform.action.start.StartTransformRequest
import org.opensearch.transport.RemoteTransportException

class AttemptCreateTransformJobStep(
    private val action: TransformAction
) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var transformId: String? = null
    private var hasTransformFailed: Boolean? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val managedIndexMetadata = context.metadata
        val previousRunTransformId = managedIndexMetadata.actionMetaData?.actionProperties?.transformActionProperties?.transformId
        val hasPreviousTransformAttemptFailed = managedIndexMetadata.actionMetaData?.actionProperties?.transformActionProperties?.hasTransformFailed

        // Creating a transform job
        val transform = action.ismTransform.toTransform(indexName, context.user)
        transformId = transform.id
        logger.info("Attempting to create a transform job $transformId for index $indexName")

        val indexTransformRequest = IndexTransformRequest(transform, WriteRequest.RefreshPolicy.IMMEDIATE)

        try {
            val response: IndexTransformResponse = context.client.suspendUntil { execute(IndexTransformAction.INSTANCE, indexTransformRequest, it) }
            logger.info("Received status ${response.status.status} on trying to create transform job $transformId")

            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(transform.id, indexName))
        } catch (e: VersionConflictEngineException) {
            val message = getFailedJobExistsMessage(transform.id, indexName)
            logger.info(message)
            if (transformId == previousRunTransformId && hasPreviousTransformAttemptFailed == true) {
                startTransformJob(transform.id, context)
            } else {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("info" to message)
            }
        } catch (e: RemoteTransportException) {
            processFailure(transform.id, indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: OpenSearchException) {
            processFailure(transform.id, indexName, e)
        } catch (e: Exception) {
            processFailure(transform.id, indexName, e)
        }

        return this
    }

    fun processFailure(transformId: String, indexName: String, e: Exception) {
        val message = getFailedMessage(transformId, indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        info = mapOf("message" to message, "cause" to "${e.message}")
    }

    private suspend fun startTransformJob(transformId: String, context: StepContext) {
        val indexName = context.metadata.index
        val client = context.client
        logger.info("Attempting to re-start the transform job $transformId")
        try {
            val startTransformRequest = StartTransformRequest(transformId)
            client.suspendUntil<Client, AcknowledgedResponse> { execute(StartTransformAction.INSTANCE, startTransformRequest, it) }
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessRestartMessage(transformId, indexName))
        } catch (e: Exception) {
            val message = getFailedToStartMessage(transformId, indexName)
            logger.error(message, e)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message)
        }
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetadata.actionMetaData
        val transformActionProperties = TransformActionProperties(transformId, hasTransformFailed)
        return currentMetadata.copy(
            actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(transformActionProperties = transformActionProperties)),
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent(): Boolean = true

    companion object {
        const val name = "attempt_create_transform"
        fun getFailedMessage(transformId: String, index: String) = "Failed to create the transform job [$transformId] [index=$index]"
        fun getFailedJobExistsMessage(transformId: String, index: String) =
            "Transform job [$transformId] already exists, skipping creation [index=$index]"
        fun getFailedToStartMessage(transformId: String, index: String) = "Failed to start the transform job [$transformId] [index=$index]"
        fun getSuccessMessage(transformId: String, index: String) = "Successfully created the transform job [$transformId] [index=$index]"
        fun getSuccessRestartMessage(transformId: String, index: String) = "Successfully restarted the transform job [$transformId] [index=$index]"
    }
}
