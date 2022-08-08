/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.metadata.MetadataCreateIndexService.validateIndexOrAliasName
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getRolloverAlias
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indices.InvalidIndexNameException

@OpenForTesting
class ValidateDelete(
    settings: Settings,
    clusterService: ClusterService
) : Validate(settings, clusterService) {

    private val logger = LogManager.getLogger(javaClass)

    override fun execute(indexName: String): Validate {
        // if these conditions are false, fail validation and do not execute delete action
        if (!deleteIndexExists(indexName) || !validIndex(indexName)) {
            return this
        }

        val (rolloverTarget, isDataStream) = getRolloverTargetOrUpdateInfo(indexName)
        rolloverTarget ?: return this

        if (isDataStream) {
            if (!notWriteIndexForDataStream(rolloverTarget, indexName)) {
                return this
            }
        }
        return this
    }

    private fun getRolloverTargetOrUpdateInfo(indexName: String): Pair<String?, Boolean> {
        val metadata = clusterService.state().metadata()
        val indexAbstraction = metadata.indicesLookup[indexName]
        val isDataStreamIndex = indexAbstraction?.parentDataStream != null

        val rolloverTarget = when {
            isDataStreamIndex -> indexAbstraction?.parentDataStream?.name
            else -> metadata.index(indexName).getRolloverAlias()
        }

        if (rolloverTarget == null) {
            val message = getFailedNoValidAliasMessage(indexName)
            logger.warn(message)
            stepStatus = Step.StepStatus.VALIDATION_FAILED
            validationStatus = ValidationStatus.RE_VALIDATING
            validationMessage = message
        }

        return rolloverTarget to isDataStreamIndex
    }

    // validation logic
    private fun notWriteIndexForDataStream(alias: String?, indexName: String): Boolean {
        val metadata = clusterService.state().metadata
        val indexAlias = metadata.index(indexName)?.aliases?.get(alias)

        val isWriteIndex = indexAlias?.writeIndex() // this could be null
        if (isWriteIndex == true) {
            val aliasIndices = metadata.indicesLookup[alias]?.indices?.map { it.index }
            logger.debug("Alias $alias contains indices $aliasIndices")
            if (aliasIndices != null && aliasIndices.size > 1) {
                val message = getFailedIsWriteIndexMessage(indexName, "testdatastream")
                logger.warn(message)
                stepStatus = Step.StepStatus.VALIDATION_FAILED
                validationStatus = ValidationStatus.RE_VALIDATING
                validationMessage = message
                return false
            }
        }
        return true
    }

    // checks if index exists
    private fun deleteIndexExists(indexName: String): Boolean {
        val indexExists = clusterService.state().metadata.indices.containsKey(indexName)
        if (!indexExists) {
            val message = getNoIndexMessage(indexName)
            logger.warn(message)
            stepStatus = Step.StepStatus.VALIDATION_FAILED
            validationStatus = ValidationStatus.RE_VALIDATING
            validationMessage = message
            return false
        }
        return true
    }

    // checks if index is valid
    private fun validIndex(indexName: String): Boolean {
        val exceptionGenerator: (String, String) -> RuntimeException = { index_name, reason -> InvalidIndexNameException(index_name, reason) }
        // If the index name is invalid for any reason, this will throw an exception giving the reason why in the message.
        // That will be displayed to the user as the cause.
        try {
            validateIndexOrAliasName(indexName, exceptionGenerator)
        } catch (e: Exception) {
            val message = getIndexNotValidMessage(indexName)
            logger.warn(message)
            stepStatus = Step.StepStatus.VALIDATION_FAILED
            validationStatus = ValidationStatus.RE_VALIDATING
            validationMessage = message
        }
        return true
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData, actionMetaData: ActionMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            actionMetaData = actionMetaData
        )
    }

    override fun validatePolicy(): Boolean {
        return true
    }

    @Suppress("TooManyFunctions")
    companion object {
        fun getNoIndexMessage(index: String) = "no such index [index=$index]"
        fun getIndexNotValidMessage(index: String) = "delete index [index=$index] not valid"
        fun getFailedIsWriteIndexMessage(index: String, dataStream: String) = "Index [index=$index] is the write index for data stream [$dataStream] and cannot be deleted"
        fun getFailedNoValidAliasMessage(index: String) = "Missing rollover_alias index setting [index=$index]"
    }
}
