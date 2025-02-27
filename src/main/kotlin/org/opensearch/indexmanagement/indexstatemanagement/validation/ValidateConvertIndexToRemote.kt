/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.metadata.MetadataCreateIndexService
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indices.InvalidIndexNameException
import org.opensearch.monitor.jvm.JvmService

@OpenForTesting
class ValidateConvertIndexToRemote(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService,
) : Validate(settings, clusterService, jvmService) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("ReturnSuppressCount", "ReturnCount")
    override fun execute(indexName: String): Validate {
        // For restore action, check if index name is valid
        if (!validIndexName(indexName)) {
            validationStatus = ValidationStatus.FAILED
            return this
        }

        // Optionally, check if the index already exists
        // Depending on your requirements, you may want to allow or disallow restoring over existing indices
        if (indexExists(indexName)) {
            val message = getIndexAlreadyExistsMessage(indexName)
            logger.warn(message)
            validationStatus = ValidationStatus.FAILED
            validationMessage = message
            return this
        }

        validationMessage = getValidationPassedMessage(indexName)
        return this
    }

    private fun indexExists(indexName: String): Boolean {
        val indexExists = clusterService.state().metadata.indices.containsKey(indexName)
        return indexExists
    }

    // Checks if the index name is valid according to OpenSearch naming conventions
    private fun validIndexName(indexName: String): Boolean {
        val exceptionGenerator: (String, String) -> RuntimeException = { name, reason ->
            InvalidIndexNameException(name, reason)
        }
        try {
            MetadataCreateIndexService.validateIndexOrAliasName(indexName, exceptionGenerator)
        } catch (e: Exception) {
            val message = getIndexNotValidMessage(indexName)
            logger.warn(message)
            validationMessage = message
            return false
        }
        return true
    }

    @Suppress("TooManyFunctions")
    companion object {
        const val name = "validate_convert_index_to_remote"
        fun getIndexAlreadyExistsMessage(index: String) = "Index [index=$index] already exists, cannot restore over existing index."
        fun getIndexNotValidMessage(index: String) = "Index [index=$index] is not valid for restore action."
        fun getValidationPassedMessage(index: String) = "Restore action validation passed for [index=$index]"
    }
}
