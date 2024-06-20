package org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics

import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.ActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry

class TransitionActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.TRANSITION
    lateinit var successes: Counter
    lateinit var failures: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Transition Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Transition Action Failures", "count")
    }

    companion object {
        val instance: TransitionActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = TransitionActionMetrics()
    }
}
