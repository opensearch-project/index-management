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

class MoveShardActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.MOVE_SHARD
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Move Shard Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Move Shard Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Move Shard Actions", "milliseconds")
    }

    companion object {
        val instance: MoveShardActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = MoveShardActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val moveShardActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.MOVE_SHARD) as MoveShardActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            moveShardActionMetrics.successes.add(1.0, context.let { moveShardActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            moveShardActionMetrics.failures.add(1.0, context.let { moveShardActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        moveShardActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { moveShardActionMetrics.createTags(it) })
    }
}
