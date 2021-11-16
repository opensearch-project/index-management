/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.cluster.node.info.NodesInfoAction
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.util.OpenForTesting

// TODO this can be moved to job scheduler, so that all extended plugin
//  can avoid running jobs in an upgrading cluster
@OpenForTesting
class SkipExecution(
    private val client: Client,
    private val clusterService: ClusterService
) : ClusterStateListener {
    private val logger = LogManager.getLogger(javaClass)

    @Volatile final var flag: Boolean = false
        private set
    // To track if there are any legacy IM plugin nodes part of the cluster
    @Volatile final var hasLegacyPlugin: Boolean = false
        private set

    init {
        clusterService.addListener(this)
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        if (event.nodesChanged() || event.isNewCluster) {
            sweepISMPluginVersion()
        }
    }

    fun sweepISMPluginVersion() {
        // if old version ISM plugin exists (2 versions ISM in one cluster), set skip flag to true
        val request = NodesInfoRequest().clear().addMetric("plugins")
        client.execute(
            NodesInfoAction.INSTANCE, request,
            object : ActionListener<NodesInfoResponse> {
                override fun onResponse(response: NodesInfoResponse) {
                    val versionSet = mutableSetOf<String>()
                    val legacyVersionSet = mutableSetOf<String>()

                    response.nodes.map { it.getInfo(PluginsAndModules::class.java).pluginInfos }
                        .forEach {
                            it.forEach { nodePlugin ->
                                if (nodePlugin.name == "opensearch-index-management" ||
                                    nodePlugin.name == "opensearch_index_management"
                                ) {
                                    versionSet.add(nodePlugin.version)
                                }

                                if (nodePlugin.name == "opendistro-index-management" ||
                                    nodePlugin.name == "opendistro_index_management"
                                ) {
                                    legacyVersionSet.add(nodePlugin.version)
                                }
                            }
                        }

                    if ((versionSet.size + legacyVersionSet.size) > 1) {
                        flag = true
                        logger.info("There are multiple versions of Index Management plugins in the cluster: [$versionSet, $legacyVersionSet]")
                    } else flag = false

                    if (versionSet.isNotEmpty() && legacyVersionSet.isNotEmpty()) {
                        hasLegacyPlugin = true
                        logger.info("Found legacy plugin versions [$legacyVersionSet] and opensearch plugins versions [$versionSet] in the cluster")
                    } else hasLegacyPlugin = false
                }

                override fun onFailure(e: Exception) {
                    logger.error("Failed sweeping nodes for ISM plugin versions: $e")
                    flag = false
                }
            }
        )
    }
}
