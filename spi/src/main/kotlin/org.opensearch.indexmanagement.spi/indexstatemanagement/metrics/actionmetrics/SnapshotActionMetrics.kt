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

class SnapshotActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.SNAPSHOT
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Snapshot Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Snapshot Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Snapshot Actions", "milliseconds")
    }

    companion object {
        val instance: SnapshotActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = SnapshotActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val snapshotActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.SNAPSHOT) as SnapshotActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            snapshotActionMetrics.successes.add(1.0, context.let { snapshotActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            snapshotActionMetrics.failures.add(1.0, context.let { snapshotActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        snapshotActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { snapshotActionMetrics.createTags(it) })
    }
}
