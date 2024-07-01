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

class CreateRollupActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.CREATE_ROLLUP
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Create Rollup Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Create Rollup Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Create Rollup Actions", "milliseconds")
    }

    companion object {
        val instance: CreateRollupActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = CreateRollupActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val createRollupActionMetrics =
            indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.CREATE_ROLLUP) as CreateRollupActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            createRollupActionMetrics.successes.add(1.0, context.let { createRollupActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            createRollupActionMetrics.failures.add(1.0, context.let { createRollupActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        createRollupActionMetrics.cumulativeLatency.add(
            latency.toDouble(),
            context.let { createRollupActionMetrics.createTags(it) },
        )
    }
}
