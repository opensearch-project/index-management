/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.util.OpenForTesting

@OpenForTesting
class ValidationService(
    val settings: Settings,
    val clusterService: ClusterService // what settings do I need
) {

    // overarching validate function
    fun validate(action: Action, currentActionMetaData: ActionMetaData, step: Step, context: StepContext): Validate {

        // map action to validation class
        val validation = when (action.type) {
            "rollover" -> ValidateRollover(settings, clusterService).executeValidation(action, currentActionMetaData, step, context)
            else -> {
                // temporary call until all actions are mapped
                ValidateRollover(settings, clusterService).executeValidation(action, currentActionMetaData, step, context)
            }
        }
        return validation
    }
}
