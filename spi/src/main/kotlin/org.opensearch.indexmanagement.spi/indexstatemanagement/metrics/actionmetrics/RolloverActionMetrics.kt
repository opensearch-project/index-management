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

class RolloverActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.ROLLOVER
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Rollover Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Rollover Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Rollover Actions", "milliseconds")
    }

    companion object {
        val instance: RolloverActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = RolloverActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val rolloverActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.ROLLOVER) as RolloverActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            rolloverActionMetrics.successes.add(1.0, context.let { rolloverActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            rolloverActionMetrics.failures.add(1.0, context.let { rolloverActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        rolloverActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { rolloverActionMetrics.createTags(it) })
    }
}
