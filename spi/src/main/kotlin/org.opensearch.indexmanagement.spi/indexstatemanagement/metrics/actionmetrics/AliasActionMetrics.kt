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

class AliasActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.ALIAS_ACTION
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Alias Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Alias Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Alias Actions", "milliseconds")
    }

    companion object {
        val instance: AliasActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = AliasActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val aliasActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.ALIAS_ACTION) as AliasActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            aliasActionMetrics.successes.add(1.0, context.let { aliasActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            aliasActionMetrics.failures.add(1.0, context.let { aliasActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        aliasActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { aliasActionMetrics.createTags(it) })
    }
}
