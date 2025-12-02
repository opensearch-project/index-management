/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.rollup

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchException
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.indexmanagement.indexstatemanagement.action.RollupAction
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupAction
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupRequest
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupResponse
import org.opensearch.indexmanagement.rollup.action.start.StartRollupAction
import org.opensearch.indexmanagement.rollup.action.start.StartRollupRequest
import org.opensearch.indexmanagement.rollup.util.RollupFieldValueExpressionResolver
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.Client

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

        try {
            // Create a temporary rollup object for template resolution context.
            // This provides the rollup's source_index as {{ctx.source_index}} in templates.
            val tempRollup = action.ismRollup.toRollup(indexName, context.user)

            // Resolve source_index template if provided, else use managed index name.
            // This enables patterns like:
            // - source_index: "{{ctx.index}}" -> resolves to the managed index name
            // - source_index: null -> defaults to the managed index name (backward compatible)
            val resolvedSourceIndex = if (action.ismRollup.sourceIndex != null) {
                RollupFieldValueExpressionResolver.resolve(
                    tempRollup,
                    action.ismRollup.sourceIndex,
                    indexName,
                )
            } else {
                indexName
            }

            // Resolve target_index template.
            // Common patterns:
            // - "rollup_{{ctx.index}}" -> "rollup_logs-2024-01"
            // - "rollup_tier2_{{ctx.index}}" -> "rollup_tier2_rollup_tier1_logs-2024-01"
            val resolvedTargetIndex = RollupFieldValueExpressionResolver.resolve(
                tempRollup,
                action.ismRollup.targetIndex,
                indexName,
            )

            // Validate resolved indices to ensure they are valid and different.
            // This catches configuration errors early before attempting to create the rollup job.
            validateResolvedIndices(resolvedSourceIndex, resolvedTargetIndex)

            logger.info(
                "Executing rollup from source [$resolvedSourceIndex] to target [$resolvedTargetIndex] " +
                    "for managed index [$indexName]",
            )

            // Create the final rollup job with resolved source_index and target_index.
            // The copy() ensures that template variables are replaced with actual index names
            // before the rollup job is persisted and executed.
            val rollup = action.ismRollup.toRollup(indexName, context.user)
                .copy(sourceIndex = resolvedSourceIndex, targetIndex = resolvedTargetIndex)
            rollupId = rollup.id
            logger.info("Attempting to create a rollup job $rollupId for index $indexName")

            val indexRollupRequest = IndexRollupRequest(rollup, WriteRequest.RefreshPolicy.IMMEDIATE)
            val response: IndexRollupResponse = context.client.suspendUntil { execute(IndexRollupAction.INSTANCE, indexRollupRequest, it) }
            logger.info("Received status ${response.status.status} on trying to create rollup job $rollupId")

            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(rollup.id, indexName))
        } catch (e: IllegalArgumentException) {
            val message = "Failed to validate resolved indices for rollup job"
            logger.error(message, e)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message, "cause" to "${e.message}")
        } catch (e: VersionConflictEngineException) {
            val message = getFailedJobExistsMessage(rollupId ?: "unknown", indexName)
            logger.info(message)
            if (rollupId == previousRunRollupId && hasPreviousRollupAttemptFailed == true) {
                startRollupJob(rollupId ?: "unknown", context)
            } else {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("info" to message)
            }
        } catch (e: RemoteTransportException) {
            processFailure(rollupId ?: "unknown", indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: OpenSearchException) {
            processFailure(rollupId ?: "unknown", indexName, e)
        } catch (e: Exception) {
            val message = "Failed to create rollup job"
            logger.error(message, e)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message, "cause" to "${e.message}")
        }

        return this
    }

    /**
     * Validates that resolved source and target indices are valid and different.
     *
     * This validation ensures that:
     * 1. The resolved source_index is not empty or whitespace-only
     * 2. The resolved target_index is not empty or whitespace-only
     * 3. The source and target indices are different (prevents self-rollup)
     *
     * @param sourceIndex The resolved source index name (after template resolution)
     * @param targetIndex The resolved target index name (after template resolution)
     * @throws IllegalArgumentException if any validation rule fails, with a descriptive error message
     */
    private fun validateResolvedIndices(sourceIndex: String, targetIndex: String) {
        require(sourceIndex.isNotBlank()) {
            "Resolved source_index cannot be empty"
        }
        require(targetIndex.isNotBlank()) {
            "Resolved target_index cannot be empty"
        }
        require(sourceIndex != targetIndex) {
            "Source and target indices must be different: $sourceIndex"
        }
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
            client.suspendUntil<Client, AcknowledgedResponse> { execute(StartRollupAction.INSTANCE, startRollupRequest, it) }
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
            info = info,
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
