/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.metrics

import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.DeleteActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.ForceMergeActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.NotificationActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.ReplicaCountActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.RolloverActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.TransitionActionMetrics
import org.opensearch.telemetry.metrics.MetricsRegistry

abstract class ActionMetrics {
    abstract val actionName: String
}

class IndexManagementActionsMetrics private constructor() {
    private lateinit var metricsRegistry: MetricsRegistry
    private lateinit var actionMetricsMap: Map<String, ActionMetrics>

    companion object {
        val instance: IndexManagementActionsMetrics by lazy { HOLDER.instance }

        const val ROLLOVER = "rollover"
        const val NOTIFICATION = "notification"
        const val FORCE_MERGE = "force_merge"
        const val DELETE = "delete"
        const val REPLICA_COUNT = "replica_count"
        const val TRANSITION = "transition"

        private object HOLDER {
            val instance = IndexManagementActionsMetrics()
        }
    }

    fun initialize(metricsRegistry: MetricsRegistry) {
        this.metricsRegistry = metricsRegistry

        RolloverActionMetrics.instance.initializeCounters(metricsRegistry)
        NotificationActionMetrics.instance.initializeCounters(metricsRegistry)
        ForceMergeActionMetrics.instance.initializeCounters(metricsRegistry)
        DeleteActionMetrics.instance.initializeCounters(metricsRegistry)
        ReplicaCountActionMetrics.instance.initializeCounters(metricsRegistry)
        TransitionActionMetrics.instance.initializeCounters(metricsRegistry)

        actionMetricsMap = mapOf(
            ROLLOVER to RolloverActionMetrics.instance,
            NOTIFICATION to NotificationActionMetrics.instance,
            FORCE_MERGE to ForceMergeActionMetrics.instance,
            DELETE to DeleteActionMetrics.instance,
            REPLICA_COUNT to ReplicaCountActionMetrics.instance,
            TRANSITION to TransitionActionMetrics.instance,
        )
    }

    fun getActionMetrics(actionName: String): ActionMetrics? {
        return actionMetricsMap[actionName]
    }
}
