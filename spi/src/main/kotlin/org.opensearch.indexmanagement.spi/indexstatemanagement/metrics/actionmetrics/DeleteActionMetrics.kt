package org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics

import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.ActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry

class DeleteActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.DELETE
    lateinit var successes: Counter
    lateinit var failures: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Delete Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Delete Action Failures", "count")
    }

    companion object {
        val instance: DeleteActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = DeleteActionMetrics()
    }
}
