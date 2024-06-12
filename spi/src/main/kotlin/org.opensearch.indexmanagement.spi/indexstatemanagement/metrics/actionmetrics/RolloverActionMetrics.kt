package org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics

import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.ActionMetrics
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry

class RolloverActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = "rollover_action"
    lateinit var successes: Counter
    lateinit var failures: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry, actionName: String) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Rollover Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Rollover Action Failures", "count")
    }

    companion object {
        val instance: RolloverActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = RolloverActionMetrics()
    }
}
