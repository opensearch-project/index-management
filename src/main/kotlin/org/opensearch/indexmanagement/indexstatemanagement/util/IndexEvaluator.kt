/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.util

import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings

class IndexEvaluator(settings: Settings, clusterService: ClusterService) {

    @Volatile private var restrictedIndexPattern = ManagedIndexSettings.RESTRICTED_INDEX_PATTERN.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN) {
            restrictedIndexPattern = it
        }
    }

    fun isUnManageableIndex(index: String): Boolean {
        return Regex(restrictedIndexPattern).matches(index)
    }

    companion object {
        const val EVALUATION_FAILURE_MESSAGE = "Matches restricted index pattern defined in the cluster setting"
    }
}
