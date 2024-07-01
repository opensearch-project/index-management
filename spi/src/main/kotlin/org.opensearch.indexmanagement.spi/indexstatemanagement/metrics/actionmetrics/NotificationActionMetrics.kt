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

class NotificationActionMetrics private constructor() : ActionMetrics() {
    override val actionName: String = IndexManagementActionsMetrics.NOTIFICATION
    lateinit var successes: Counter
    lateinit var failures: Counter
    lateinit var cumulativeLatency: Counter

    fun initializeCounters(metricsRegistry: MetricsRegistry) {
        successes = metricsRegistry.createCounter("${actionName}_successes", "Notification Action Successes", "count")
        failures = metricsRegistry.createCounter("${actionName}_failures", "Notification Action Failures", "count")
        cumulativeLatency = metricsRegistry.createCounter("${actionName}_cumulative_latency", "Cumulative Latency of Notification Action", "milliseconds")
    }

    companion object {
        val instance: NotificationActionMetrics by lazy { HOLDER.instance }
    }

    private object HOLDER {
        val instance = NotificationActionMetrics()
    }

    override fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    ) {
        val notificationActionMetrics = indexManagementActionsMetrics.getActionMetrics(IndexManagementActionsMetrics.NOTIFICATION) as NotificationActionMetrics
        val stepStatus = stepMetaData?.stepStatus
        if (stepStatus == StepStatus.COMPLETED) {
            notificationActionMetrics.successes.add(1.0, context.let { notificationActionMetrics.createTags(it) })
        }
        if (stepStatus == StepStatus.FAILED) {
            notificationActionMetrics.failures.add(1.0, context.let { notificationActionMetrics.createTags(it) })
        }
        val endTime = System.currentTimeMillis()
        val latency = endTime - (context.metadata.stepMetaData?.startTime ?: endTime)
        notificationActionMetrics.cumulativeLatency.add(latency.toDouble(), context.let { notificationActionMetrics.createTags(it) })
    }
}
