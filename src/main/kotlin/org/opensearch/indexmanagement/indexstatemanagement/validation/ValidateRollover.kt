/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getRolloverAlias
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.util.OpenForTesting

@OpenForTesting
class ValidateRollover(
    settings: Settings,
    clusterService: ClusterService
) : Validate(settings, clusterService) {

    private val logger = LogManager.getLogger(javaClass)
    private var info: Map<String, Any>? = null

    // pass in action, action meta data, step, and step context
    // returns a validate object with updated validation and step status
    override fun executeValidation(action: Action, currentActionMetaData: ActionMetaData, step: Step, context: StepContext): Validate {

        val (rolloverTarget, isDataStream) = getRolloverTargetOrUpdateInfo(context)
        // If the rolloverTarget is null, we would've already updated the failed info from getRolloverTargetOrUpdateInfo and can return early
        // rolloverTarget ?: return this

        if (!isDataStream) {
            if (!hasAlias(context, rolloverTarget) || !isWriteIndex(context, rolloverTarget)) {
                return this
            }
        }

        return this
    }

    // validation logic------------------------------------------------------------------------------------------------

    private fun hasAlias(context: StepContext, alias: String?): Boolean {
        val indexName = context.metadata.index
        val metadata = context.clusterService.state().metadata
        val indexAlias = metadata.index(indexName)?.aliases?.get(alias)

        logger.debug("Index $indexName has aliases $indexAlias")
        if (indexAlias == null) {
            stepStatus = Step.StepStatus.FAILED
            validationStatus = ValidationStatus.REVALIDATE
            info = mapOf("message" to getMissingAliasMessage(indexName))
            return false
        }
        return true
    }

    private fun isWriteIndex(context: StepContext, alias: String?): Boolean {
        val indexName = context.metadata.index
        val metadata = context.clusterService.state().metadata
        val indexAlias = metadata.index(indexName)?.aliases?.get(alias)

        val isWriteIndex = indexAlias?.writeIndex() // this could be null
        if (isWriteIndex != true) {
            val aliasIndices = metadata.indicesLookup[alias]?.indices?.map { it.index }
            logger.debug("Alias $alias contains indices $aliasIndices")
            if (aliasIndices != null && aliasIndices.size > 1) {
                stepStatus = Step.StepStatus.FAILED
                validationStatus = ValidationStatus.REVALIDATE
                info = mapOf("message" to getFailedWriteIndexMessage(indexName))
                return false
            }
        }
        return true
    }

    // write more failure messages
    private fun getRolloverTargetOrUpdateInfo(context: StepContext): Pair<String?, Boolean> {
        val indexName = context.metadata.index
        val metadata = context.clusterService.state().metadata()
        val indexAbstraction = metadata.indicesLookup[indexName]
        val isDataStreamIndex = indexAbstraction?.parentDataStream != null

        val rolloverTarget = when {
            isDataStreamIndex -> indexAbstraction?.parentDataStream?.name
            else -> metadata.index(indexName).getRolloverAlias()
        }

        if (rolloverTarget == null) {
            val message = AttemptRolloverStep.getFailedNoValidAliasMessage(indexName)
            logger.warn(message)
            stepStatus = Step.StepStatus.FAILED
            info = mapOf("message" to message)
        }

        return rolloverTarget to isDataStreamIndex
    }

    // TODO: 7/18/22
    override fun validatePolicy(): Boolean {
        return true
    }

    // TODO: 7/18/22
    // validate generic errors like if index exists
//    override fun validateGeneric(): Boolean {
//
//        return true
//    }

    @Suppress("TooManyFunctions")
    companion object {
        fun getFailedMessage(index: String) = "Failed to rollover index [index=$index]"
        fun getFailedWriteIndexMessage(index: String) = "Not the write index when rollover [index=$index]"
        fun getMissingAliasMessage(index: String) = "Missing alias when rollover [index=$index]"
        fun getFailedAliasUpdateMessage(index: String, newIndex: String) =
            "New index created, but failed to update alias [index=$index, newIndex=$newIndex]"
        fun getFailedDataStreamRolloverMessage(dataStream: String) = "Failed to rollover data stream [data_stream=$dataStream]"
        fun getFailedNoValidAliasMessage(index: String) = "Missing rollover_alias index setting [index=$index]"
        fun getFailedEvaluateMessage(index: String) = "Failed to evaluate conditions for rollover [index=$index]"
        fun getPendingMessage(index: String) = "Pending rollover of index [index=$index]"
        fun getAlreadyRolledOverMessage(index: String, alias: String) =
            "This index has already been rolled over using this alias, treating as a success [index=$index, alias=$alias]"
    }
}
