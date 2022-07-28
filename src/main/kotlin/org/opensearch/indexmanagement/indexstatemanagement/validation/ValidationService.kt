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

        // map action to validation class
        val validation = when (action.type) {
            "rollover" -> ValidateRollover(settings, clusterService).executeValidation(context)
            "delete" -> ValidateDelete(settings, clusterService).executeValidation(context)
            else -> {
                // temporary call until all actions are mapped
                ValidateNothing(settings, clusterService).executeValidation(context)
            }
        }
        return validation
    }
}
