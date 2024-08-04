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

class ShrinkActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.SHRINK
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Shrink Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Shrink Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Shrink Actions", "milliseconds")
    }

    companion object {
        val instance: ShrinkActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = ShrinkActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val shrinkActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.SHRINK) as ShrinkActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            shrinkActionMetrics.successes.add(1.0, context.let { shrinkActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            shrinkActionMetrics.failures.add(1.0, context.let { shrinkActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        shrinkActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { shrinkActionMetrics.createTags(it) })
    }
}
