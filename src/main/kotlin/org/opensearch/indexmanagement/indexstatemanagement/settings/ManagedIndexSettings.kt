/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.settings

import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import org.opensearch.common.settings.Setting
import org.opensearch.common.unit.TimeValue
import java.util.function.Function

class ManagedIndexSettings {
    companion object {
        const val DEFAULT_ISM_ENABLED = true
        const val DEFAULT_METADATA_SERVICE_ENABLED = true
        const val DEFAULT_JOB_INTERVAL = 5
        private val ALLOW_LIST_ALL = ActionConfig.ActionType.values().toList().map { it.type }
        val ALLOW_LIST_NONE = emptyList<String>()
        val SNAPSHOT_DENY_LIST_NONE = emptyList<String>()
        const val HOST_DENY_LIST = "opendistro.destination.host.deny_list"

        val INDEX_STATE_MANAGEMENT_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "plugins.index_state_management.enabled",
            LegacyOpenDistroManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
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
    }
}
