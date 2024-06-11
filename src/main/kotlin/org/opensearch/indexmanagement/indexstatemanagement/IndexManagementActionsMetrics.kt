package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.indexmanagement.indexstatemanagement.actionmetrics.RolloverActionMetrics
import org.opensearch.telemetry.metrics.MetricsRegistry

abstract class ActionMetrics {
    abstract val actionName: String
}

class IndexManagementActionsMetrics private constructor() {
    private lateinit var metricsRegistry: MetricsRegistry
    private lateinit var actionMetricsMap: Map<String, ActionMetrics>

    fun initialize(metricsRegistry: MetricsRegistry) {
        this.metricsRegistry = metricsRegistry
        actionMetricsMap = mapOf(
            "rollover" to RolloverActionMetrics(metricsRegistry),
            // Add other action metrics here
        )
    }

    fun getActionMetrics(actionName: String): ActionMetrics? {
        return actionMetricsMap[actionName]
    }

    companion object {
        val instance: IndexManagementActionsMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = IndexManagementActionsMetrics()
    }
}
