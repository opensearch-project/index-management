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

class ReplicaCountActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.REPLICA_COUNT
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Replica Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Replica Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Replica Count Action", "milliseconds")
    }

    companion object {
        val instance: ReplicaCountActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = ReplicaCountActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val replicaCountActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.REPLICA_COUNT) as ReplicaCountActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            replicaCountActionMetrics.successes.add(1.0, context.let { replicaCountActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            replicaCountActionMetrics.failures.add(1.0, context.let { replicaCountActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        replicaCountActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { replicaCountActionMetrics.createTags(it) })
    }
}
