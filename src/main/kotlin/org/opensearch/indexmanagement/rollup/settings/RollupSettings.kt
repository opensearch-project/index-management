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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.indexmanagement.rollup.settings

import org.opensearch.common.settings.Setting
import org.opensearch.common.unit.TimeValue

class RollupSettings {

    companion object {
        const val DEFAULT_ROLLUP_ENABLED = true
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

        val ROLLUP_DASHBOARDS: Setting<Boolean> = Setting.boolSetting(
            "plugins.rollup.dashboards.enabled",
            LegacyOpenDistroRollupSettings.ROLLUP_DASHBOARDS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
