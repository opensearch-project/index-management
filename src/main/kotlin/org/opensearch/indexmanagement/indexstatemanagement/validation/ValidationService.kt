/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.util.OpenForTesting

@OpenForTesting
class ValidationService(
    val settings: Settings,
    val clusterService: ClusterService
) {

    // overarching validate function
    fun validate(action: Action, context: StepContext): Validate {
        val indexName = context.metadata.index
        // map action to validation class
        val validation = when (action.type) {
            "rollover" -> ValidateRollover(settings, clusterService).executeValidation(indexName)
            "delete" -> ValidateDelete(settings, clusterService).executeValidation(indexName)
            else -> {
                // temporary call until all actions are mapped
                ValidateNothing(settings, clusterService).executeValidation(indexName)
            }
        }
        return validation
    }
}
