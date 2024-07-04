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

class AllocationActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.ALLOCATION
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Allocation Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Allocation Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Allocation Actions", "milliseconds")
    }

    companion object {
        val instance: AllocationActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = AllocationActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val allocationActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.ALLOCATION) as AllocationActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            allocationActionMetrics.successes.add(1.0, context.let { allocationActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            allocationActionMetrics.failures.add(1.0, context.let { allocationActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        allocationActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { allocationActionMetrics.createTags(it) })
    }
}
