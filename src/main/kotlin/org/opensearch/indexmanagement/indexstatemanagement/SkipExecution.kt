/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
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
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool

// TODO this can be moved to job scheduler, so that all extended plugin
//  can avoid running jobs in an upgrading cluster
@OpenForTesting
class SkipExecution(
    private val settings: Settings,
    private val threadPool: ThreadPool,
    private val client: Client,
    private val clusterService: ClusterService,
) : ClusterStateListener,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("ISMPluginSweepCoordinator")),
    LifecycleListener() {
    private val logger = LogManager.getLogger(javaClass)

    private var scheduledSkipExecution: Scheduler.Cancellable? = null

    @Volatile
    private var sweepSkipPeriod = ManagedIndexSettings.SWEEP_SKIP_PERIOD.get(settings)

    @Volatile
    private var indexStateManagementEnabled = ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.get(settings)

    @Volatile
    final var flag: Boolean = false
        private set

    // To track if there are any legacy IM plugin nodes part of the cluster
    @Volatile
    final var hasLegacyPlugin: Boolean = false
        private set

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.SWEEP_SKIP_PERIOD) {
            sweepSkipPeriod = it
            initBackgroundSweepISMPluginVersionExecution()
        }
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        if (event.nodesChanged() || event.isNewCluster) {
            sweepISMPluginVersion()
        }
    }

    override fun beforeStop() {
        scheduledSkipExecution?.cancel()
    }

    @OpenForTesting
    fun initBackgroundSweepISMPluginVersionExecution() {
        logger.info("initing sweep skip execution")
        // If ISM is disabled return early
        if (!isIndexStateManagementEnabled()) return
        // Cancel existing background sweep
        scheduledSkipExecution?.cancel()
        val scheduledJob = Runnable {
            launch {
                try {
                    if (!flag) {
                        logger.info("Canceling sweep ism plugin version")
                        scheduledSkipExecution?.cancel()
                        return@launch
                    }
                    sweepISMPluginVersion()
                } catch (e: Exception) {
                    logger.error("Failed to sweep managed indices", e)
                }
            }
        }
        scheduledSkipExecution =
            threadPool.scheduleWithFixedDelay(scheduledJob, TimeValue.timeValueMinutes(5), ThreadPool.Names.MANAGEMENT)
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

    private fun isIndexStateManagementEnabled(): Boolean = indexStateManagementEnabled == true
}
