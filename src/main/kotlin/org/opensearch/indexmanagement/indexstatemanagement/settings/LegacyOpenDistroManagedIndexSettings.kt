/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.settings

import org.opensearch.common.settings.Setting
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.DEFAULT_ISM_ENABLED
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.DEFAULT_JOB_INTERVAL
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.ALLOW_LIST_ALL
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.SNAPSHOT_DENY_LIST_NONE
import java.util.concurrent.TimeUnit
import java.util.function.Function

@Suppress("UtilityClassWithPublicConstructor")
class LegacyOpenDistroManagedIndexSettings {
    companion object {
        val INDEX_STATE_MANAGEMENT_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "opendistro.index_state_management.enabled",
            DEFAULT_ISM_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val POLICY_ID: Setting<String> = Setting.simpleString(
            "index.opendistro.index_state_management.policy_id",
            Setting.Property.IndexScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val ROLLOVER_ALIAS: Setting<String> = Setting.simpleString(
            "index.opendistro.index_state_management.rollover_alias",
            Setting.Property.IndexScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val ROLLOVER_SKIP: Setting<Boolean> = Setting.boolSetting(
            "index.opendistro.index_state_management.rollover_skip",
            false,
            Setting.Property.IndexScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val AUTO_MANAGE: Setting<Boolean> = Setting.boolSetting(
            "index.opendistro.index_state_management.auto_manage",
            true,
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        )

        val JOB_INTERVAL: Setting<Int> = Setting.intSetting(
            "opendistro.index_state_management.job_interval",
            DEFAULT_JOB_INTERVAL,
            1,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val SWEEP_PERIOD: Setting<TimeValue> = Setting.timeSetting(
            "opendistro.index_state_management.coordinator.sweep_period",
            TimeValue.timeValueMinutes(10),
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val COORDINATOR_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "opendistro.index_state_management.coordinator.backoff_millis",
            TimeValue.timeValueMillis(50),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val COORDINATOR_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "opendistro.index_state_management.coordinator.backoff_count",
            2,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val HISTORY_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "opendistro.index_state_management.history.enabled",
            true,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val HISTORY_MAX_DOCS: Setting<Long> = Setting.longSetting(
            "opendistro.index_state_management.history.max_docs",
            2500000L, // 1 doc is ~10kb or less. This many doc is roughly 25gb
            0L,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val HISTORY_INDEX_MAX_AGE: Setting<TimeValue> = Setting.positiveTimeSetting(
            "opendistro.index_state_management.history.max_age",
            TimeValue.timeValueHours(24),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val HISTORY_ROLLOVER_CHECK_PERIOD: Setting<TimeValue> = Setting.positiveTimeSetting(
            "opendistro.index_state_management.history.rollover_check_period",
            TimeValue.timeValueHours(8),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val HISTORY_RETENTION_PERIOD: Setting<TimeValue> = Setting.positiveTimeSetting(
            "opendistro.index_state_management.history.rollover_retention_period",
            TimeValue(30, TimeUnit.DAYS),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val HISTORY_NUMBER_OF_SHARDS: Setting<Int> = Setting.intSetting(
            "opendistro.index_state_management.history.number_of_shards",
            1,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val HISTORY_NUMBER_OF_REPLICAS: Setting<Int> = Setting.intSetting(
            "opendistro.index_state_management.history.number_of_replicas",
            1,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val ALLOW_LIST: Setting<List<String>> = Setting.listSetting(
            "opendistro.index_state_management.allow_list",
            ALLOW_LIST_ALL,
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val SNAPSHOT_DENY_LIST: Setting<List<String>> = Setting.listSetting(
            "opendistro.index_state_management.snapshot.deny_list",
            SNAPSHOT_DENY_LIST_NONE,
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val RESTRICTED_INDEX_PATTERN = Setting.simpleString(
            "opendistro.index_state_management.restricted_index_pattern",
            ManagedIndexSettings.DEFAULT_RESTRICTED_PATTERN,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )
    }
}
