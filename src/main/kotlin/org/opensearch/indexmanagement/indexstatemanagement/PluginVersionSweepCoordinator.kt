/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.util.OpenForTesting
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool

class PluginVersionSweepCoordinator(
    private val skipExecution: SkipExecution,
    settings: Settings,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
) : CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("ISMPluginSweepCoordinator")),
    LifecycleListener() {
    private val logger = LogManager.getLogger(javaClass)

    private var scheduledSkipExecution: Scheduler.Cancellable? = null

    @Volatile
    private var sweepSkipPeriod = ManagedIndexSettings.SWEEP_SKIP_PERIOD.get(settings)

    @Volatile
    private var indexStateManagementEnabled = ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.get(settings)

    init {
        clusterService.addLifecycleListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ManagedIndexSettings.SWEEP_SKIP_PERIOD) {
            sweepSkipPeriod = it
            initBackgroundSweepISMPluginVersionExecution()
        }
    }

    override fun afterStart() {
        initBackgroundSweepISMPluginVersionExecution()
    }

    override fun beforeStop() {
        scheduledSkipExecution?.cancel()
    }

    @OpenForTesting
    fun initBackgroundSweepISMPluginVersionExecution() {
        // If ISM is disabled return early
        if (!isIndexStateManagementEnabled()) return
        // Cancel existing background sweep
        scheduledSkipExecution?.cancel()
        val scheduledJob = Runnable {
            // Starting job without coroutine - in order to avoid thread leak error
            try {
                if (!skipExecution.flag) {
                    logger.info("Canceling sweep ism plugin version job")
                    scheduledSkipExecution?.cancel()
                } else {
                    skipExecution.sweepISMPluginVersion()
                }
            } catch (e: Exception) {
                logger.error("Failed to sweep ism plugin version", e)
            }
        }
        scheduledSkipExecution =
            threadPool.scheduleWithFixedDelay(scheduledJob,
                TimeValue.timeValueMinutes(RETRY_PERIOD_IN_MINUTES),
                ThreadPool.Names.MANAGEMENT)
    }

    private fun isIndexStateManagementEnabled(): Boolean = indexStateManagementEnabled == true

    companion object {
        private const val RETRY_PERIOD_IN_MINUTES = 5L
    }
}
