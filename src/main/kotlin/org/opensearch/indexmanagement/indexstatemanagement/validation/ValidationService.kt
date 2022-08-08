/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ValidationResult

@OpenForTesting
class ValidationService(
    val settings: Settings,
    val clusterService: ClusterService
) {

    // overarching validate function
    fun validate(action: Action, indexName: String): ValidationResult {
        // map action to validation class
        val validation = when (action.type) {
            "rollover" -> ValidateRollover(settings, clusterService).execute(indexName)
            "delete" -> ValidateDelete(settings, clusterService).execute(indexName)
            "force_merge" -> ValidateForceMerge(settings, clusterService).execute(indexName)
            else -> {
                // temporary call until all actions are mapped
                ValidateNothing(settings, clusterService).execute(indexName)
            }
        }
//        logger.info("i have reached here")
//        logger.info(validation.validationMessage)
//        logger.info(action)
//        logger.info(indexName)
        return ValidationResult(validation.validationMessage.toString(), validation.validationStatus)
    }
}
