/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics

import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.ActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry

class ForceMergeActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.FORCE_MERGE
    lateinit var successes: Counter
    lateinit var failures: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes =
            metricsRegistry.createCounter("${actionName}_successes", "Force Merge Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Force Merge Action Failures", "count")
    }

    companion object {
        val instance: ForceMergeActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = ForceMergeActionMetrics()
    }
}
