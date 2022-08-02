/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ValidationMetaData

@OpenForTesting
class ValidationService(
    val settings: Settings,
    val clusterService: ClusterService
) {

    // overarching validate function
    fun validate(action: Action, indexName: String): ValidationMetaData {
        // map action to validation class
        val validation = when (action.type) {
            "rollover" -> ValidateRollover(settings, clusterService).executeValidation(indexName)
            "delete" -> ValidateDelete(settings, clusterService).executeValidation(indexName)
            else -> {
                // temporary call until all actions are mapped
                ValidateNothing(settings, clusterService).executeValidation(indexName)
            }
        }

        return ValidationMetaData(validation.validationMessage.toString(), validation.validationStatus)
    }

    fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData, actionMetaData: ActionMetaData, validationMetaData: ValidationMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            actionMetaData = actionMetaData,
            validationMetaData = validationMetaData
        )
    }
}
