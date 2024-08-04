/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics

import org.opensearch.indexmanagement.spi.indexstatemanagement.Step.StepStatus
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.ActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry

class ForceMergeActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.FORCE_MERGE
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Force Merge Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Force Merge Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Force Merge Action", "milliseconds")
    }

    companion object {
        val instance: ForceMergeActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = ForceMergeActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val forceMergeActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.FORCE_MERGE) as ForceMergeActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            forceMergeActionMetrics.successes.add(1.0, context.let { forceMergeActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            forceMergeActionMetrics.failures.add(1.0, context.let { forceMergeActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        forceMergeActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { forceMergeActionMetrics.createTags(it) })
    }
}
