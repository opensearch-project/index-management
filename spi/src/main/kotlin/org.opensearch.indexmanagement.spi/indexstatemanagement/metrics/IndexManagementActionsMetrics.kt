/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.metrics

import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.AliasActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.AllocationActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.CloseActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.CreateRollupActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.CreateTransformActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.DeleteActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.ForceMergeActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.MoveShardActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.NotificationActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.OpenActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.ReplicaCountActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.RolloverActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.SetIndexPriorityActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.SetReadOnlyActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.ShrinkActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.SnapshotActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.TransitionActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.telemetry.metrics.MetricsRegistry
import org.opensearch.telemetry.metrics.tags.Tags

abstract class ActionMetrics {
    abstract val actionName: String

    fun createTags(context: StepContext): Tags {
        val tags = Tags.create()
            .addTag("index_name", context.metadata.index)
            .addTag("policy_id", context.metadata.policyID)
            .addTag("node_id", context.clusterService.nodeName ?: "")
            .addTag("Index_uuid", context.metadata.indexUuid)
        return tags
    }

    abstract fun emitMetrics(
        context: StepContext,
        indexManagementActionsMetrics: IndexManagementActionsMetrics,
        stepMetaData: StepMetaData?,
    )
}

class IndexManagementActionsMetrics private constructor() {
    private lateinit var metricsRegistry: MetricsRegistry
    private lateinit var actionMetricsMap: Map<String, ActionMetrics>

    companion object {
        val instance: IndexManagementActionsMetrics by lazy { HOLDER.instance }

        const val ROLLOVER = "rollover"
        const val NOTIFICATION = "notification"
        const val FORCE_MERGE = "force_merge"
        const val DELETE = "delete"
        const val REPLICA_COUNT = "replica_count"
        const val TRANSITION = "transition"
        const val CLOSE = "close"
        const val SET_INDEX_PRIORITY = "set_index_priority"
        const val OPEN = "open"
        const val CREATE_ROLLUP = "create_rollup"
        const val CREATE_TRANSFORM = "create_transform"
        const val MOVE_SHARD = "move_shard"
        const val SET_READ_ONLY = "set_read_only"
        const val SHRINK = "shrink"
        const val SNAPSHOT = "snapshot"
        const val ALIAS_ACTION = "alias_action"
        const val ALLOCATION = "allocation"

        private object HOLDER {
            val instance = IndexManagementActionsMetrics()
        }
    }

    fun initialize(metricsRegistry: MetricsRegistry) {
        this.metricsRegistry = metricsRegistry

        RolloverActionMetrics.instance.initializeCounters(metricsRegistry)
        NotificationActionMetrics.instance.initializeCounters(metricsRegistry)
        ForceMergeActionMetrics.instance.initializeCounters(metricsRegistry)
        DeleteActionMetrics.instance.initializeCounters(metricsRegistry)
        ReplicaCountActionMetrics.instance.initializeCounters(metricsRegistry)
        TransitionActionMetrics.instance.initializeCounters(metricsRegistry)
        CloseActionMetrics.instance.initializeCounters(metricsRegistry)
        SetIndexPriorityActionMetrics.instance.initializeCounters(metricsRegistry)
        OpenActionMetrics.instance.initializeCounters(metricsRegistry)
        CreateRollupActionMetrics.instance.initializeCounters(metricsRegistry)
        CreateTransformActionMetrics.instance.initializeCounters(metricsRegistry)
        MoveShardActionMetrics.instance.initializeCounters(metricsRegistry)
        SetReadOnlyActionMetrics.instance.initializeCounters(metricsRegistry)
        ShrinkActionMetrics.instance.initializeCounters(metricsRegistry)
        SnapshotActionMetrics.instance.initializeCounters(metricsRegistry)
        AliasActionMetrics.instance.initializeCounters(metricsRegistry)
        AllocationActionMetrics.instance.initializeCounters(metricsRegistry)

        actionMetricsMap = mapOf(
            ROLLOVER to RolloverActionMetrics.instance,
            NOTIFICATION to NotificationActionMetrics.instance,
            FORCE_MERGE to ForceMergeActionMetrics.instance,
            DELETE to DeleteActionMetrics.instance,
            REPLICA_COUNT to ReplicaCountActionMetrics.instance,
            TRANSITION to TransitionActionMetrics.instance,
            CLOSE to CloseActionMetrics.instance,
            SET_INDEX_PRIORITY to SetIndexPriorityActionMetrics.instance,
            OPEN to OpenActionMetrics.instance,
            CREATE_ROLLUP to CreateRollupActionMetrics.instance,
            CREATE_TRANSFORM to CreateTransformActionMetrics.instance,
            MOVE_SHARD to MoveShardActionMetrics.instance,
            SET_READ_ONLY to SetReadOnlyActionMetrics.instance,
            SHRINK to ShrinkActionMetrics.instance,
            SNAPSHOT to SnapshotActionMetrics.instance,
            ALIAS_ACTION to AliasActionMetrics.instance,
            ALLOCATION to AllocationActionMetrics.instance,
        )
    }

    fun getActionMetrics(actionName: String): ActionMetrics? {
        return actionMetricsMap[actionName]
    }
}
