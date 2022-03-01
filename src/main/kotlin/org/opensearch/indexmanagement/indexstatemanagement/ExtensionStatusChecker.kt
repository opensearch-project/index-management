/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.spi.indexstatemanagement.Status
import org.opensearch.indexmanagement.spi.indexstatemanagement.StatusChecker

/**
 * Check the extension status check. The extension status should be used to represent if the extension is turned on or off,
 * not as a health check denoting availability.
 */
class ExtensionStatusChecker(private val extensionCheckers: Map<String, StatusChecker>, val clusterService: ClusterService) {

    fun isEnabled(extensionName: String?): Boolean {
        val checker = extensionCheckers[extensionName] ?: return true
        val clusterState = clusterService.state()
        return checker.check(clusterState) == Status.ENABLED
    }
}
