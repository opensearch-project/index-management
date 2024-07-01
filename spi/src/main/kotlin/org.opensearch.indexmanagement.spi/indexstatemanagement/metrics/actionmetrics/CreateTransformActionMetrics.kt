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

class CreateTransformActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.CREATE_TRANSFORM
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Create Transform Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Create Transform Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Create Transform Actions", "milliseconds")
    }

    companion object {
        val instance: CreateTransformActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = CreateTransformActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val createTransformActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.CREATE_TRANSFORM) as CreateRollupActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            createTransformActionMetrics.successes.add(1.0, context.let { createTransformActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            createTransformActionMetrics.failures.add(1.0, context.let { createTransformActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        createTransformActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { createTransformActionMetrics.createTags(it) })
    }
}
