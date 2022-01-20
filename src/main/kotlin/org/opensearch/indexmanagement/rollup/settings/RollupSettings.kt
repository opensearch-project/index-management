/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.settings

import org.opensearch.common.settings.Setting
import org.opensearch.common.unit.TimeValue

@Suppress("UtilityClassWithPublicConstructor")
class RollupSettings {

    companion object {
        const val DEFAULT_ROLLUP_ENABLED = true
        const val DEFAULT_SEARCH_ALL_JOBS = false
        const val DEFAULT_ACQUIRE_LOCK_RETRY_COUNT = 3
        const val DEFAULT_ACQUIRE_LOCK_RETRY_DELAY = 1000L
        const val DEFAULT_RENEW_LOCK_RETRY_COUNT = 3
        const val DEFAULT_RENEW_LOCK_RETRY_DELAY = 1000L
        const val DEFAULT_CLIENT_REQUEST_RETRY_COUNT = 3
        const val DEFAULT_CLIENT_REQUEST_RETRY_DELAY = 1000L

        val ROLLUP_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "plugins.rollup.enabled",
            LegacyOpenDistroRollupSettings.ROLLUP_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_SEARCH_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "plugins.rollup.search.enabled",
            LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_INDEX: Setting<Boolean> = Setting.boolSetting(
            "index.plugins.rollup_index",
            LegacyOpenDistroRollupSettings.ROLLUP_INDEX,
            Setting.Property.IndexScope,
            Setting.Property.InternalIndex
        )

        val ROLLUP_INGEST_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "plugins.rollup.ingest.backoff_millis",
            LegacyOpenDistroRollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_INGEST_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "plugins.rollup.ingest.backoff_count",
            LegacyOpenDistroRollupSettings.ROLLUP_INGEST_BACKOFF_COUNT,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_SEARCH_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "plugins.rollup.search.backoff_millis",
            LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_BACKOFF_MILLIS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_SEARCH_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "plugins.rollup.search.backoff_count",
            LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_BACKOFF_COUNT,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_SEARCH_ALL_JOBS: Setting<Boolean> = Setting.boolSetting(
            "plugins.rollup.search.search_all_jobs",
            DEFAULT_SEARCH_ALL_JOBS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_DASHBOARDS: Setting<Boolean> = Setting.boolSetting(
            "plugins.rollup.dashboards.enabled",
            LegacyOpenDistroRollupSettings.ROLLUP_DASHBOARDS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
