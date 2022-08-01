/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate

@OpenForTesting
class ValidateNothing(
    settings: Settings,
    clusterService: ClusterService
) : Validate(settings, clusterService) {

    // skips validation
    override fun executeValidation(indexName: String): Validate {
        return this
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData, actionMetaData: ActionMetaData): ManagedIndexMetaData {
        return currentMetadata.copy()
    }

    override fun validatePolicy(): Boolean {
        return true
    }
}
