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

class SetIndexPriorityActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.SET_INDEX_PRIORITY
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Set Index Priority Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Set Index Priority Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Set Index Priority Actions", "milliseconds")
    }

    companion object {
        val instance: SetIndexPriorityActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = SetIndexPriorityActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val setIndexPriorityActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.SET_INDEX_PRIORITY) as SetIndexPriorityActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            setIndexPriorityActionMetrics.successes.add(1.0, context.let { setIndexPriorityActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            setIndexPriorityActionMetrics.failures.add(1.0, context.let { setIndexPriorityActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        setIndexPriorityActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { setIndexPriorityActionMetrics.createTags(it) })
    }
}
