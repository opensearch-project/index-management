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

class DeleteActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.DELETE
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Delete Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Delete Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Delete Action", "milliseconds")
    }

    companion object {
        val instance: DeleteActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = DeleteActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val deleteActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.DELETE) as DeleteActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            deleteActionMetrics.successes.add(1.0, context.let { deleteActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            deleteActionMetrics.failures.add(1.0, context.let { deleteActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        deleteActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { deleteActionMetrics.createTags(it) })
    }
}
