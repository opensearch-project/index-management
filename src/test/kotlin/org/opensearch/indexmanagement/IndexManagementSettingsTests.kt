/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.junit.Before
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.settings.LegacyOpenDistroManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.rollup.settings.LegacyOpenDistroRollupSettings
import org.opensearch.indexmanagement.rollup.settings.RollupSettings
import org.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings
import org.opensearch.test.OpenSearchTestCase

class IndexManagementSettingsTests : OpenSearchTestCase() {

    private lateinit var plugin: IndexManagementPlugin

    @Before
    fun setup() {
        plugin = IndexManagementPlugin()
    }

    fun testAllLegacyOpenDistroSettingsReturned() {
        val settings = plugin.settings
        assertTrue(
            "legacy setting must be returned from settings",
            settings.containsAll(
                listOf<Any>(
                    LegacyOpenDistroManagedIndexSettings.HISTORY_ENABLED,
                    LegacyOpenDistroManagedIndexSettings.HISTORY_INDEX_MAX_AGE,
                    LegacyOpenDistroManagedIndexSettings.HISTORY_MAX_DOCS,
                    LegacyOpenDistroManagedIndexSettings.HISTORY_RETENTION_PERIOD,
                    LegacyOpenDistroManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD,
                    LegacyOpenDistroManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS,
                    LegacyOpenDistroManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS,
                    LegacyOpenDistroManagedIndexSettings.POLICY_ID,
                    LegacyOpenDistroManagedIndexSettings.ROLLOVER_ALIAS,
                    LegacyOpenDistroManagedIndexSettings.ROLLOVER_SKIP,
                    LegacyOpenDistroManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
                    LegacyOpenDistroManagedIndexSettings.METADATA_SERVICE_ENABLED,
                    LegacyOpenDistroManagedIndexSettings.JOB_INTERVAL,
                    LegacyOpenDistroManagedIndexSettings.SWEEP_PERIOD,
                    LegacyOpenDistroManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
                    LegacyOpenDistroManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
                    LegacyOpenDistroManagedIndexSettings.ALLOW_LIST,
                    LegacyOpenDistroManagedIndexSettings.SNAPSHOT_DENY_LIST,
                    LegacyOpenDistroManagedIndexSettings.RESTRICTED_INDEX_PATTERN,
                    LegacyOpenDistroRollupSettings.ROLLUP_INGEST_BACKOFF_COUNT,
                    LegacyOpenDistroRollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS,
                    LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_BACKOFF_COUNT,
                    LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_BACKOFF_MILLIS,
                    LegacyOpenDistroRollupSettings.ROLLUP_INDEX,
                    LegacyOpenDistroRollupSettings.ROLLUP_ENABLED,
                    LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_ENABLED,
                    LegacyOpenDistroRollupSettings.ROLLUP_DASHBOARDS
                )
            )
        )
    }

    fun testAllOpenSearchSettingsReturned() {
        val settings = plugin.settings
        assertTrue(
            "opensearch setting must be returned from settings",
            settings.containsAll(
                listOf<Any>(
                    ManagedIndexSettings.HISTORY_ENABLED,
                    ManagedIndexSettings.HISTORY_INDEX_MAX_AGE,
                    ManagedIndexSettings.HISTORY_MAX_DOCS,
                    ManagedIndexSettings.HISTORY_RETENTION_PERIOD,
                    ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD,
                    ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS,
                    ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS,
                    ManagedIndexSettings.POLICY_ID,
                    ManagedIndexSettings.ROLLOVER_ALIAS,
                    ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
                    ManagedIndexSettings.METADATA_SERVICE_ENABLED,
                    ManagedIndexSettings.JOB_INTERVAL,
                    ManagedIndexSettings.SWEEP_PERIOD,
                    ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
                    ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
                    ManagedIndexSettings.ALLOW_LIST,
                    ManagedIndexSettings.SNAPSHOT_DENY_LIST,
                    ManagedIndexSettings.JITTER,
                    RollupSettings.ROLLUP_INGEST_BACKOFF_COUNT,
                    RollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS,
                    RollupSettings.ROLLUP_SEARCH_BACKOFF_COUNT,
                    RollupSettings.ROLLUP_SEARCH_BACKOFF_MILLIS,
                    RollupSettings.ROLLUP_INDEX,
                    RollupSettings.ROLLUP_ENABLED,
                    RollupSettings.ROLLUP_SEARCH_ENABLED,
                    RollupSettings.ROLLUP_SEARCH_ALL_JOBS,
                    RollupSettings.ROLLUP_DASHBOARDS,
                    SnapshotManagementSettings.FILTER_BY_BACKEND_ROLES
                )
            )
        )
    }

    fun testLegacyOpenDistroSettingsFallback() {
        assertEquals(
            ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.get(Settings.EMPTY),
            LegacyOpenDistroManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.get(Settings.EMPTY)
        )
    }

    fun testSettingsGetValue() {
        val settings = Settings.builder().put("plugins.index_state_management.job_interval", "1").build()
        assertEquals(ManagedIndexSettings.JOB_INTERVAL.get(settings), 1)
        assertEquals(LegacyOpenDistroManagedIndexSettings.JOB_INTERVAL.get(settings), 5)
    }

    fun testIndexSettingLegacyFallback() {
        var settings = Settings.builder()
            .put("index.opendistro.index_state_management.rollover_skip", true)
            .build()
        assertEquals(ManagedIndexSettings.ROLLOVER_SKIP.get(settings), true)

        settings = Settings.builder()
            .put("index.opendistro.index_state_management.rollover_skip", true)
            .put("index.plugins.index_state_management.rollover_skip", false)
            .build()
        assertEquals(ManagedIndexSettings.ROLLOVER_SKIP.get(settings), false)

        assertSettingDeprecationsAndWarnings(
            arrayOf(LegacyOpenDistroManagedIndexSettings.ROLLOVER_SKIP)
        )
    }

    fun testSettingsGetValueWithLegacyFallback() {
        val settings = Settings.builder()
            .put("opendistro.index_state_management.enabled", false)
            .put("opendistro.index_state_management.metadata_service.enabled", false)
            .put("opendistro.index_state_management.job_interval", 1)
            .put("opendistro.index_state_management.coordinator.sweep_period", "6m")
            .put("opendistro.index_state_management.coordinator.backoff_millis", "1ms")
            .put("opendistro.index_state_management.coordinator.backoff_count", 1)
            .put("opendistro.index_state_management.history.enabled", false)
            .put("opendistro.index_state_management.history.max_docs", 1L)
            .put("opendistro.index_state_management.history.max_age", "1m")
            .put("opendistro.index_state_management.history.rollover_check_period", "1m")
            .put("opendistro.index_state_management.history.rollover_retention_period", "1m")
            .put("opendistro.index_state_management.history.number_of_shards", 2)
            .put("opendistro.index_state_management.history.number_of_replicas", 2)
            .putList("opendistro.index_state_management.allow_list", listOf("1"))
            .putList("opendistro.index_state_management.snapshot.deny_list", listOf("1"))
            .put("opendistro.index_state_management.restricted_index_pattern", "blocked_index_pattern")
            .put("opendistro.rollup.enabled", false)
            .put("opendistro.rollup.search.enabled", false)
            .put("opendistro.rollup.ingest.backoff_millis", "1ms")
            .put("opendistro.rollup.ingest.backoff_count", 1)
            .put("opendistro.rollup.search.backoff_millis", "1ms")
            .put("opendistro.rollup.search.backoff_count", 1)
            .put("opendistro.rollup.dashboards.enabled", false)
            .build()

        assertEquals(ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED.get(settings), false)
        assertEquals(ManagedIndexSettings.METADATA_SERVICE_ENABLED.get(settings), false)
        assertEquals(ManagedIndexSettings.JOB_INTERVAL.get(settings), 1)
        assertEquals(ManagedIndexSettings.SWEEP_PERIOD.get(settings), TimeValue.timeValueMinutes(6))
        assertEquals(ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS.get(settings), TimeValue.timeValueMillis(1))
        assertEquals(ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT.get(settings), 1)
        assertEquals(ManagedIndexSettings.HISTORY_ENABLED.get(settings), false)
        assertEquals(ManagedIndexSettings.HISTORY_MAX_DOCS.get(settings), 1L)
        assertEquals(ManagedIndexSettings.HISTORY_INDEX_MAX_AGE.get(settings), TimeValue.timeValueMinutes(1))
        assertEquals(ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD.get(settings), TimeValue.timeValueMinutes(1))
        assertEquals(ManagedIndexSettings.HISTORY_RETENTION_PERIOD.get(settings), TimeValue.timeValueMinutes(1))
        assertEquals(ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS.get(settings), 2)
        assertEquals(ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS.get(settings), 2)
        assertEquals(ManagedIndexSettings.ALLOW_LIST.get(settings), listOf("1"))
        assertEquals(ManagedIndexSettings.SNAPSHOT_DENY_LIST.get(settings), listOf("1"))
        assertEquals(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN.get(settings), "blocked_index_pattern")
        assertEquals(RollupSettings.ROLLUP_ENABLED.get(settings), false)
        assertEquals(RollupSettings.ROLLUP_SEARCH_ENABLED.get(settings), false)
        assertEquals(RollupSettings.ROLLUP_SEARCH_ALL_JOBS.get(settings), false)
        assertEquals(RollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS.get(settings), TimeValue.timeValueMillis(1))
        assertEquals(RollupSettings.ROLLUP_INGEST_BACKOFF_COUNT.get(settings), 1)
        assertEquals(RollupSettings.ROLLUP_SEARCH_BACKOFF_MILLIS.get(settings), TimeValue.timeValueMillis(1))
        assertEquals(RollupSettings.ROLLUP_SEARCH_BACKOFF_COUNT.get(settings), 1)
        assertEquals(RollupSettings.ROLLUP_DASHBOARDS.get(settings), false)

        assertSettingDeprecationsAndWarnings(
            arrayOf(
                LegacyOpenDistroManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
                LegacyOpenDistroManagedIndexSettings.METADATA_SERVICE_ENABLED,
                LegacyOpenDistroManagedIndexSettings.JOB_INTERVAL,
                LegacyOpenDistroManagedIndexSettings.SWEEP_PERIOD,
                LegacyOpenDistroManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
                LegacyOpenDistroManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
                LegacyOpenDistroManagedIndexSettings.HISTORY_ENABLED,
                LegacyOpenDistroManagedIndexSettings.HISTORY_MAX_DOCS,
                LegacyOpenDistroManagedIndexSettings.HISTORY_INDEX_MAX_AGE,
                LegacyOpenDistroManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD,
                LegacyOpenDistroManagedIndexSettings.HISTORY_RETENTION_PERIOD,
                LegacyOpenDistroManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS,
                LegacyOpenDistroManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS,
                LegacyOpenDistroManagedIndexSettings.ALLOW_LIST,
                LegacyOpenDistroManagedIndexSettings.SNAPSHOT_DENY_LIST,
                LegacyOpenDistroManagedIndexSettings.RESTRICTED_INDEX_PATTERN,
                LegacyOpenDistroRollupSettings.ROLLUP_ENABLED,
                LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_ENABLED,
                LegacyOpenDistroRollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS,
                LegacyOpenDistroRollupSettings.ROLLUP_INGEST_BACKOFF_COUNT,
                LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_BACKOFF_MILLIS,
                LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_BACKOFF_COUNT,
                LegacyOpenDistroRollupSettings.ROLLUP_DASHBOARDS
            )
        )
    }
}
