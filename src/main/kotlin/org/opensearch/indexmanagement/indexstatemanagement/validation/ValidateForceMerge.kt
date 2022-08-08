/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate

@OpenForTesting
class ValidateForceMerge(
    settings: Settings,
    clusterService: ClusterService
) : Validate(settings, clusterService) {

    private val logger = LogManager.getLogger(javaClass)

    // returns a Validate object with updated validation and step status
    @Suppress("ReturnSuppressCount", "ReturnCount")
    override fun execute(indexName: String): Validate {
        logger.info("inside force merge")

        validationMessage = getValidationPassedMessage(indexName)
        return this
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData, actionMetaData: ActionMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            actionMetaData = actionMetaData
        )
    }

    // TODO: 7/18/22
    override fun validatePolicy(): Boolean {
        return true
    }

    @Suppress("TooManyFunctions")
    companion object {
        const val name = "validate_force_merge"
        fun getValidationPassedMessage(index: String) = "Force Merge validation passed for [index=$index]"
    }
}
