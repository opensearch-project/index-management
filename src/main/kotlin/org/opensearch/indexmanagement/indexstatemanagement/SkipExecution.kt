/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.Version
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.util.OpenForTesting

// TODO this can be moved to job scheduler, so that all extended plugin
//  can avoid running jobs in an upgrading cluster
@OpenForTesting
class SkipExecution {
    private val logger = LogManager.getLogger(javaClass)

    @Volatile
    final var flag: Boolean = false
        private set

    // To track if there are any legacy IM plugin nodes part of the cluster
    @Volatile
    final var hasLegacyPlugin: Boolean = false
        private set

    fun sweepISMPluginVersion(clusterService: ClusterService) {
        try {
            // if old version ISM plugin exists (2 versions ISM in one cluster), set skip flag to true
            val currentMinVersion = clusterService.state().nodes.minNodeVersion
            val currentMaxVersion = clusterService.state().nodes.maxNodeVersion

            if (currentMinVersion != null && !currentMinVersion.equals(currentMaxVersion)) {
                flag = true
                logger.info("There are multiple versions of Index Management plugins in the cluster: [$currentMaxVersion, $currentMinVersion]")
            } else {
                flag = false
            }

            if (currentMinVersion.major > Version.CURRENT.major && currentMinVersion != currentMaxVersion) {
                hasLegacyPlugin = true
                logger.info("Found legacy plugin versions [$currentMinVersion] and opensearch plugins versions [$currentMaxVersion] in the cluster")
            } else {
                hasLegacyPlugin = false
            }
        } catch (e: Exception) {
            logger.error("Unable to fetch node versions from cluster service", e)
        }
    }
}
