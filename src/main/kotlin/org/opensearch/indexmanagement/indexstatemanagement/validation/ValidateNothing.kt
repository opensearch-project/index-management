/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.util.OpenForTesting

@OpenForTesting
class ValidateNothing(
    settings: Settings,
    clusterService: ClusterService
) : Validate(settings, clusterService) {

    // skips validation
    override fun executeValidation(context: StepContext): Validate {
        return this
    }

    override fun validatePolicy(): Boolean {
        return true
    }
}
