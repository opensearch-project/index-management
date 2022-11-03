/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.settings

import org.opensearch.common.settings.Setting
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import java.util.function.Function

@Suppress("UtilityClassWithPublicConstructor")
class ManagedIndexSettings {
    companion object {
        const val DEFAULT_ISM_ENABLED = true
        const val DEFAULT_ACTION_VALIDATION_ENABLED = false
        const val DEFAULT_TEMPLATE_MIGRATION_TIMESTAMP = 0L
        const val DEFAULT_JOB_INTERVAL = 5
        const val DEFAULT_JITTER = 0.6
        const val DEFAULT_RESTRICTED_PATTERN = "\\.opendistro_security|\\.kibana.*|\\$INDEX_MANAGEMENT_INDEX"
        val ALLOW_LIST_NONE = emptyList<String>()
        val SNAPSHOT_DENY_LIST_NONE = emptyList<String>()

        val INDEX_STATE_MANAGEMENT_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "plugins.index_state_management.enabled",
            LegacyOpenDistroManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ACTION_VALIDATION_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "plugins.index_state_management.action_validation.enabled",
            DEFAULT_ACTION_VALIDATION_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        // 0: migration is going on
        // 1: migration succeed
        // -1: migration failed
        val METADATA_SERVICE_STATUS: Setting<Int> = Setting.intSetting(
            "plugins.index_state_management.metadata_migration.status",
            LegacyOpenDistroManagedIndexSettings.METADATA_SERVICE_STATUS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        // 0: enabled, use onClusterManager time as ISM template last_updated_time
        // -1: migration ended successfully
        // -2: migration ended unsuccessfully
        // >0: use this setting (epoch millis) as ISM template last_updated_time
        val TEMPLATE_MIGRATION_CONTROL: Setting<Long> = Setting.longSetting(
            "plugins.index_state_management.template_migration.control",
            LegacyOpenDistroManagedIndexSettings.TEMPLATE_MIGRATION_CONTROL,
            -2L,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val METADATA_SERVICE_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "plugins.index_state_management.metadata_service.enabled",
            LegacyOpenDistroManagedIndexSettings.METADATA_SERVICE_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val POLICY_ID: Setting<String> = Setting.simpleString(
            "index.plugins.index_state_management.policy_id",
            LegacyOpenDistroManagedIndexSettings.POLICY_ID,
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        )

        val ROLLOVER_ALIAS: Setting<String> = Setting.simpleString(
            "index.plugins.index_state_management.rollover_alias",
            LegacyOpenDistroManagedIndexSettings.ROLLOVER_ALIAS,
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        )

        val ROLLOVER_SKIP: Setting<Boolean> = Setting.boolSetting(
            "index.plugins.index_state_management.rollover_skip",
            LegacyOpenDistroManagedIndexSettings.ROLLOVER_SKIP,
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        )

        val AUTO_MANAGE: Setting<Boolean> = Setting.boolSetting(
            "index.plugins.index_state_management.auto_manage",
            LegacyOpenDistroManagedIndexSettings.AUTO_MANAGE,
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        )

        val JOB_INTERVAL: Setting<Int> = Setting.intSetting(
            "plugins.index_state_management.job_interval",
            LegacyOpenDistroManagedIndexSettings.JOB_INTERVAL,
            1,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val SWEEP_PERIOD: Setting<TimeValue> = Setting.timeSetting(
            "plugins.index_state_management.coordinator.sweep_period",
            LegacyOpenDistroManagedIndexSettings.SWEEP_PERIOD,
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val SWEEP_SKIP_PERIOD: Setting<TimeValue> = Setting.timeSetting(
            "plugins.index_state_management.coordinator.sweep_skip_period",
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val COORDINATOR_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "plugins.index_state_management.coordinator.backoff_millis",
            LegacyOpenDistroManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val COORDINATOR_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "plugins.index_state_management.coordinator.backoff_count",
            LegacyOpenDistroManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "plugins.index_state_management.history.enabled",
            LegacyOpenDistroManagedIndexSettings.HISTORY_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_MAX_DOCS: Setting<Long> = Setting.longSetting(
            "plugins.index_state_management.history.max_docs",
            LegacyOpenDistroManagedIndexSettings.HISTORY_MAX_DOCS, // 1 doc is ~10kb or less. This many doc is roughly 25gb
            0L,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_INDEX_MAX_AGE: Setting<TimeValue> = Setting.positiveTimeSetting(
            "plugins.index_state_management.history.max_age",
            LegacyOpenDistroManagedIndexSettings.HISTORY_INDEX_MAX_AGE,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_ROLLOVER_CHECK_PERIOD: Setting<TimeValue> = Setting.positiveTimeSetting(
            "plugins.index_state_management.history.rollover_check_period",
            LegacyOpenDistroManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_RETENTION_PERIOD: Setting<TimeValue> = Setting.positiveTimeSetting(
            "plugins.index_state_management.history.rollover_retention_period",
            LegacyOpenDistroManagedIndexSettings.HISTORY_RETENTION_PERIOD,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_NUMBER_OF_SHARDS: Setting<Int> = Setting.intSetting(
            "plugins.index_state_management.history.number_of_shards",
            LegacyOpenDistroManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_NUMBER_OF_REPLICAS: Setting<Int> = Setting.intSetting(
            "plugins.index_state_management.history.number_of_replicas",
            LegacyOpenDistroManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ALLOW_LIST: Setting<List<String>> = Setting.listSetting(
            "plugins.index_state_management.allow_list",
            LegacyOpenDistroManagedIndexSettings.ALLOW_LIST,
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val SNAPSHOT_DENY_LIST: Setting<List<String>> = Setting.listSetting(
            "plugins.index_state_management.snapshot.deny_list",
            LegacyOpenDistroManagedIndexSettings.SNAPSHOT_DENY_LIST,
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val JITTER: Setting<Double> = Setting.doubleSetting(
            "plugins.index_state_management.jitter",
            DEFAULT_JITTER,
            0.0,
            1.0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val RESTRICTED_INDEX_PATTERN = Setting.simpleString(
            "plugins.index_state_management.restricted_index_pattern",
            LegacyOpenDistroManagedIndexSettings.RESTRICTED_INDEX_PATTERN,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
