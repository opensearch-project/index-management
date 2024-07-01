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

class TransitionActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.TRANSITION
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Transition Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Transition Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Transition Actions", "milliseconds")
    }

    companion object {
        val instance: TransitionActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = TransitionActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val transitionActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.TRANSITION) as TransitionActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            transitionActionMetrics.successes.add(1.0, context.let { transitionActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            transitionActionMetrics.failures.add(1.0, context.let { transitionActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        transitionActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { transitionActionMetrics.createTags(it) })
    }
}
