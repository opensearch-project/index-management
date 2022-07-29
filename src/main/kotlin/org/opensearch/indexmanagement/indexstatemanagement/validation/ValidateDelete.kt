/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.metadata.MetadataCreateIndexService.validateIndexOrAliasName
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indices.InvalidIndexNameException

@OpenForTesting
class ValidateDelete(
    settings: Settings,
    clusterService: ClusterService
) : Validate(settings, clusterService) {

    private val logger = LogManager.getLogger(javaClass)

    override fun executeValidation(indexName: String): Validate {

        // if these conditions are false, fail validation and do not execute delete action
        if (!deleteIndexExists(indexName) || !validIndex(indexName)) {
            return this
        }
        return this
    }

    // validation logic
    private fun notWriteIndexForDataStream(context: StepContext): Boolean {
        val indexName = context.metadata.index
        return true
    }

    // checks if index exists
    private fun deleteIndexExists(indexName: String): Boolean {
        val indexExists = clusterService.state().metadata.indices.containsKey(indexName)
        if (!indexExists) {
            stepStatus = Step.StepStatus.VALIDATION_FAILED
            validationStatus = ValidationStatus.REVALIDATE
            validationInfo = mapOf("message" to getNoIndexMessage(indexName))
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
            stepStatus = Step.StepStatus.VALIDATION_FAILED
            validationStatus = ValidationStatus.REVALIDATE
            validationInfo = mapOf("message" to getIndexNotValidMessage(indexName))
        }
        return true
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData, actionMetaData: ActionMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            actionMetaData = actionMetaData,
            info = validationInfo
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
    }
}
