/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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

                    response.nodes.map { it.getInfo(PluginsAndModules::class.java).pluginInfos }
                        .forEach {
                            it.forEach { nodePlugin ->
                                if (nodePlugin.name == "opensearch-index-management" ||
                                    nodePlugin.name == "opensearch_index_management"
                                ) {
                                    versionSet.add(nodePlugin.version)
                                }
                            }
                        }

                    if (versionSet.size > 1) {
                        flag = true
                        logger.info("There are multiple versions of Index Management plugins in the cluster: $versionSet")
                    } else flag = false
                }

                override fun onFailure(e: Exception) {
                    logger.error("Failed sweeping nodes for ISM plugin versions: $e")
                    flag = false
                }
            }
        )
    }
}
