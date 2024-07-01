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

class SetReadOnlyActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.SET_READ_ONLY
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Set Read Only Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Set Read Only Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Set Read Only Actions", "milliseconds")
    }

    companion object {
        val instance: SetReadOnlyActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = SetReadOnlyActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val setReadOnlyActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.SET_READ_ONLY) as SetReadOnlyActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            setReadOnlyActionMetrics.successes.add(1.0, context.let { setReadOnlyActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            setReadOnlyActionMetrics.failures.add(1.0, context.let { setReadOnlyActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        setReadOnlyActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { setReadOnlyActionMetrics.createTags(it) })
    }
}
