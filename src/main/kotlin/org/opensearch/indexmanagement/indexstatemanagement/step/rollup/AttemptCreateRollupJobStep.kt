/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.rollup

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchException
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.indexmanagement.indexstatemanagement.action.RollupAction
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupAction
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupRequest
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupResponse
import org.opensearch.indexmanagement.rollup.action.start.StartRollupAction
import org.opensearch.indexmanagement.rollup.action.start.StartRollupRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException

class AttemptCreateRollupJobStep(private val action: RollupAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var rollupId: String? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val managedIndexMetadata = context.metadata
        val previousRunRollupId = managedIndexMetadata.actionMetaData?.actionProperties?.rollupId
        val hasPreviousRollupAttemptFailed = managedIndexMetadata.actionMetaData?.actionProperties?.hasRollupFailed

        // Creating a rollup job
        val rollup = action.ismRollup.toRollup(indexName, context.user)
        rollupId = rollup.id
        logger.info("Attempting to create a rollup job $rollupId for index $indexName")

        val indexRollupRequest = IndexRollupRequest(rollup, WriteRequest.RefreshPolicy.IMMEDIATE)

        try {
            val response: IndexRollupResponse = context.client.suspendUntil { execute(IndexRollupAction.INSTANCE, indexRollupRequest, it) }
            logger.info("Received status ${response.status.status} on trying to create rollup job $rollupId")

            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(rollup.id, indexName))
        } catch (e: VersionConflictEngineException) {
            val message = getFailedJobExistsMessage(rollup.id, indexName)
            logger.info(message)
            if (rollupId == previousRunRollupId && hasPreviousRollupAttemptFailed == true) {
                startRollupJob(rollup.id, context)
            } else {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("info" to message)
            }
        } catch (e: RemoteTransportException) {
            processFailure(rollup.id, indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: OpenSearchException) {
            processFailure(rollup.id, indexName, e)
        } catch (e: Exception) {
            processFailure(rollup.id, indexName, e)
        }

        return this
    }

    fun processFailure(rollupId: String, indexName: String, e: Exception) {
        val message = getFailedMessage(rollupId, indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        info = mapOf("message" to message, "cause" to "${e.message}")
    }

    private suspend fun startRollupJob(rollupId: String, context: StepContext) {
        val indexName = context.metadata.index
        val client = context.client
        logger.info("Attempting to re-start the job $rollupId")
        try {
            val startRollupRequest = StartRollupRequest(rollupId)
            val response: AcknowledgedResponse = client.suspendUntil { execute(StartRollupAction.INSTANCE, startRollupRequest, it) }
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessRestartMessage(rollupId, indexName))
        } catch (e: Exception) {
            val message = getFailedToStartMessage(rollupId, indexName)
            logger.error(message, e)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message)
        }
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetadata.actionMetaData
        return currentMetadata.copy(
            actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(rollupId = rollupId)),
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent(): Boolean = true

    companion object {
        const val name = "attempt_create_rollup"
        fun getFailedMessage(rollupId: String, index: String) = "Failed to create the rollup job [$rollupId] [index=$index]"
        fun getFailedJobExistsMessage(rollupId: String, index: String) = "Rollup job [$rollupId] already exists, skipping creation [index=$index]"
        fun getFailedToStartMessage(rollupId: String, index: String) = "Failed to start the rollup job [$rollupId] [index=$index]"
        fun getSuccessMessage(rollupId: String, index: String) = "Successfully created the rollup job [$rollupId] [index=$index]"
        fun getSuccessRestartMessage(rollupId: String, index: String) = "Successfully restarted the rollup job [$rollupId] [index=$index]"
    }
}
