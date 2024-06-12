package org.opensearch.indexmanagement.spi.indexstatemanagement.metrics

import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.RolloverActionMetrics
import org.opensearch.telemetry.metrics.MetricsRegistry

abstract class ActionMetrics {
    abstract val actionName: String
}

class IndexManagementActionsMetrics private constructor() {
    private lateinit var metricsRegistry: MetricsRegistry
    private lateinit var actionMetricsMap: Map<String, ActionMetrics>

    fun initialize(metricsRegistry: MetricsRegistry) {
        this.metricsRegistry = metricsRegistry

        RolloverActionMetrics.instance.initializeCounters(metricsRegistry, "rollover")

        actionMetricsMap = mapOf(
            "rollover" to RolloverActionMetrics.instance,
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
