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
class ValidateClose(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService
) : Validate(settings, clusterService, jvmService) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("ReturnSuppressCount", "ReturnCount")
    override fun execute(indexName: String): Validate {
        // if these conditions are false, fail validation and do not execute close action
        if (!indexExists(indexName) || !validIndex(indexName)) {
            validationStatus = ValidationStatus.FAILED
            return this
        }
        validationMessage = getValidationPassedMessage(indexName)
        return this
    }

    private fun indexExists(indexName: String): Boolean {
        val isIndexExists = clusterService.state().metadata.indices.containsKey(indexName)
        if (!isIndexExists) {
            val message = getNoIndexMessage(indexName)
            logger.warn(message)
            validationMessage = message
            return false
        }
        return true
    }

    private fun validIndex(indexName: String): Boolean {
        val exceptionGenerator: (String, String) -> RuntimeException = { index_name, reason -> InvalidIndexNameException(index_name, reason) }
        // If the index name is invalid for any reason, this will throw an exception giving the reason why in the message.
        // That will be displayed to the user as the cause.
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
        const val name = "validate_close"
        fun getNoIndexMessage(index: String) = "No such index [index=$index] for close action."
        fun getIndexNotValidMessage(index: String) = "Index [index=$index] is not valid. Abort close action on it."
        fun getValidationPassedMessage(index: String) = "Close action validation passed for [index=$index]"
    }
}
