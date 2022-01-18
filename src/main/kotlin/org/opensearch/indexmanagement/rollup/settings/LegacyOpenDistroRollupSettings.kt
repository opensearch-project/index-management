/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.settings

import org.opensearch.common.settings.Setting
import org.opensearch.common.unit.TimeValue

@Suppress("UtilityClassWithPublicConstructor")
class LegacyOpenDistroRollupSettings {

    companion object {
        const val DEFAULT_ROLLUP_ENABLED = true
        const val DEFAULT_ACQUIRE_LOCK_RETRY_COUNT = 3
        const val DEFAULT_ACQUIRE_LOCK_RETRY_DELAY = 1000L
        const val DEFAULT_RENEW_LOCK_RETRY_COUNT = 3
        const val DEFAULT_RENEW_LOCK_RETRY_DELAY = 1000L
        const val DEFAULT_CLIENT_REQUEST_RETRY_COUNT = 3
        const val DEFAULT_CLIENT_REQUEST_RETRY_DELAY = 1000L

        val ROLLUP_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "opendistro.rollup.enabled",
            DEFAULT_ROLLUP_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val ROLLUP_SEARCH_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "opendistro.rollup.search.enabled",
            true,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val ROLLUP_INDEX: Setting<Boolean> = Setting.boolSetting(
            "index.opendistro.rollup_index",
            false,
            Setting.Property.IndexScope,
            Setting.Property.InternalIndex,
            Setting.Property.Deprecated
        )

        val ROLLUP_INGEST_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "opendistro.rollup.ingest.backoff_millis",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val ROLLUP_INGEST_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "opendistro.rollup.ingest.backoff_count",
            5,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val ROLLUP_SEARCH_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "opendistro.rollup.search.backoff_millis",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val ROLLUP_SEARCH_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "opendistro.rollup.search.backoff_count",
            5,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val ROLLUP_DASHBOARDS: Setting<Boolean> = Setting.boolSetting(
            "opendistro.rollup.dashboards.enabled",
            DEFAULT_ROLLUP_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )
    }
}
