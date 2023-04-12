/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

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
class ValidateSnapshot(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService
) : Validate(settings, clusterService, jvmService) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("ReturnSuppressCount", "ReturnCount")
    override fun execute(indexName: String): Validate {
        // if these conditions are false, fail validation and do not execute snapshot action
        if (!indexExists(indexName) || !validIndex(indexName)) {
            validationStatus = ValidationStatus.FAILED
            return this
        }
        validationMessage = getValidationPassedMessage(indexName)
        return this
    }

    private fun indexExists(indexName: String): Boolean {
        val indexExists = clusterService.state().metadata.indices.containsKey(indexName)
        if (!indexExists) {
            val message = getNoIndexMessage(indexName)
            logger.warn(message)
            validationMessage = message
            return false
        }
        return true
    }

    // checks if index is valid
    private fun validIndex(indexName: String): Boolean {
        val exceptionGenerator: (String, String) -> RuntimeException = { index_name, reason -> InvalidIndexNameException(index_name, reason) }
        try {
            MetadataCreateIndexService.validateIndexOrAliasName(indexName, exceptionGenerator)
        } catch (e: Exception) {
            val message = getIndexNotValidMessage(indexName)
            logger.warn(message)
            validationMessage = message
        }
        return true
    }

    @Suppress("TooManyFunctions")
    companion object {
        const val name = "validate_snapshot"
        fun getNoIndexMessage(index: String) = "Index [index=$index] does not exist for snapshot action."
        fun getIndexNotValidMessage(index: String) = "Index [index=$index] is not valid for snapshot action."
        fun getValidationPassedMessage(index: String) = "Snapshot action validation passed for [index=$index]"
    }
}
