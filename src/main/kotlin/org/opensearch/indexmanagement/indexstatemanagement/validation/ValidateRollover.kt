/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getRolloverAlias
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getRolloverSkip
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.monitor.jvm.JvmService

@OpenForTesting
class ValidateRollover(
    settings: Settings,
    clusterService: ClusterService,
    jvmService: JvmService
) : Validate(settings, clusterService, jvmService) {

    private val logger = LogManager.getLogger(javaClass)

    // returns a Validate object with updated validation and step status
    @Suppress("ReturnSuppressCount", "ReturnCount")
    override fun execute(indexName: String): Validate {
        val (rolloverTarget, isDataStream) = getRolloverTargetOrUpdateInfo(indexName)
        rolloverTarget ?: return this

        if (skipRollover(indexName) || alreadyRolledOver(rolloverTarget, indexName)) return this

        if (!isDataStream) {
            if (!hasAlias(rolloverTarget, indexName) || !isWriteIndex(rolloverTarget, indexName)
            ) {
                return this
            }
        }
        validationMessage = getValidationPassedMessage(indexName)
        return this
    }

    private fun skipRollover(indexName: String): Boolean {
        val skipRollover = clusterService.state().metadata.index(indexName).getRolloverSkip()
        if (skipRollover) {
            validationStatus = ValidationStatus.PASSED
            validationMessage = getSkipRolloverMessage(indexName)
            return true
        }
        return false
    }

    private fun alreadyRolledOver(alias: String?, indexName: String): Boolean {
        if (clusterService.state().metadata.index(indexName).rolloverInfos.containsKey(alias)) {
            validationStatus = ValidationStatus.PASSED
            validationMessage = getAlreadyRolledOverMessage(indexName, alias)
            return true
        }
        return false
    }

    private fun hasAlias(alias: String?, indexName: String): Boolean {
        val metadata = clusterService.state().metadata
        val indexAlias = metadata.index(indexName)?.aliases?.get(alias)

        logger.debug("Index $indexName has aliases $indexAlias")
        if (indexAlias == null) {
            val message = getMissingAliasMessage(indexName)
            logger.warn(message)
            validationStatus = ValidationStatus.RE_VALIDATING
            validationMessage = message
            return false
        }
        return true
    }

    private fun isWriteIndex(alias: String?, indexName: String): Boolean {
        val metadata = clusterService.state().metadata
        val indexAlias = metadata.index(indexName)?.aliases?.get(alias)

        val isWriteIndex = indexAlias?.writeIndex()
        if (isWriteIndex != true) {
            val aliasIndices = metadata.indicesLookup[alias]?.indices?.map { it.index }
            logger.debug("Alias $alias contains indices $aliasIndices")
            if (aliasIndices != null && aliasIndices.size > 1) {
                val message = getFailedWriteIndexMessage(indexName)
                logger.warn(message)
                validationStatus = ValidationStatus.RE_VALIDATING
                validationMessage = message
                return false
            }
        }
        return true
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
            validationStatus = ValidationStatus.RE_VALIDATING
            validationMessage = message
        }

        return rolloverTarget to isDataStreamIndex
    }

    @Suppress("TooManyFunctions")
    companion object {
        const val name = "validate_rollover"
        fun getFailedWriteIndexMessage(index: String) = "Not the write index when rollover [index=$index]"
        fun getMissingAliasMessage(index: String) = "Missing alias when rollover [index=$index]"
        fun getFailedNoValidAliasMessage(index: String) = "Missing rollover_alias index setting [index=$index]"
        fun getAlreadyRolledOverMessage(index: String, alias: String?) =
            "This index has already been rolled over using this alias, treating as a success [index=$index, alias=$alias]"
        fun getSkipRolloverMessage(index: String) = "Skipped rollover action for [index=$index]"
        fun getValidationPassedMessage(index: String) = "Rollover validation passed for [index=$index]"
    }
}
