/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.runner

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.client.ResponseException
import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.rollup.RollupRestTestCase
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.RollupMetrics
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.toJsonString
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import java.time.Instant
import java.time.temporal.ChronoUnit

@Suppress("UNCHECKED_CAST")
class RollupRoutingIT : RollupRestTestCase() {

    fun `test rollup with routing field applies routing to target documents`() {
        val sourceIndex = "routing-source-index"
        val targetIndex = "routing-target-index"

        // Create source index
        createIndex(
            sourceIndex,
            Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build(),
            """"properties": {"timestamp": {"type": "date"}, "category": {"type": "keyword"}, "value": {"type": "double"}}""",
        )

        // Index documents - 2 alpha docs in same hour, 2 beta docs in same hour
        indexDoc(sourceIndex, "1", """{"timestamp": "2023-05-03T10:00:00Z", "category": "alpha", "value": 100}""")
        indexDoc(sourceIndex, "2", """{"timestamp": "2023-05-03T10:30:00Z", "category": "alpha", "value": 150}""")
        indexDoc(sourceIndex, "3", """{"timestamp": "2023-05-03T11:00:00Z", "category": "beta", "value": 200}""")
        indexDoc(sourceIndex, "4", """{"timestamp": "2023-05-03T11:30:00Z", "category": "beta", "value": 250}""")

        // Create rollup with routing_field and multi-shard target
        val rollup = Rollup(
            id = "routing_field_test",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Test routing field",
            sourceIndex = sourceIndex,
            targetIndex = targetIndex,
            targetIndexSettings = Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 0)
                .build(),
            metadataID = null,
            roles = emptyList(),
            pageSize = 1000,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                Terms(sourceField = "category", targetField = "category"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "value", targetField = "value", metrics = listOf(Sum(), Min(), Max())),
            ),
            routingField = "category",
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        // Wait for rollup to finish
        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val metadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, metadata.status)
            assertEquals("Unexpected documents processed", 4L, metadata.stats.documentsProcessed)
            assertEquals("Unexpected rollups indexed", 2L, metadata.stats.rollupsIndexed)
        }

        // Verify routing field is stored correctly
        val fetchedRollup = getRollup(rollup.id)
        assertEquals("Routing field should be category", "category", fetchedRollup.routingField)
    }

    fun `test rollup without routing field does not set routing`() {
        val sourceIndex = "no-routing-source-index"
        val targetIndex = "no-routing-target-index"

        // Create source index
        createIndex(
            sourceIndex,
            Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build(),
            """"properties": {"timestamp": {"type": "date"}, "category": {"type": "keyword"}, "value": {"type": "double"}}""",
        )

        // Index documents
        indexDoc(sourceIndex, "1", """{"timestamp": "2023-05-03T10:00:00Z", "category": "alpha", "value": 100}""")
        indexDoc(sourceIndex, "2", """{"timestamp": "2023-05-03T11:00:00Z", "category": "beta", "value": 200}""")

        // Create rollup WITHOUT routing_field
        val rollup = Rollup(
            id = "no_routing_field_test",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Test no routing field",
            sourceIndex = sourceIndex,
            targetIndex = targetIndex,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 1000,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                Terms(sourceField = "category", targetField = "category"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "value", targetField = "value", metrics = listOf(Sum())),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        // Wait for rollup to finish
        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val metadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, metadata.status)
        }

        // Verify the rollup job does not have routing_field
        val fetchedRollup = getRollup(rollup.id)
        assertNull("Routing field should be null", fetchedRollup.routingField)
    }

    fun `test rollup routing field cannot be modified after creation`() {
        val sourceIndex = "immutable-routing-source"
        val targetIndex = "immutable-routing-target"

        createIndex(
            sourceIndex,
            Settings.EMPTY,
            """"properties": {"timestamp": {"type": "date"}, "category": {"type": "keyword"}, "value": {"type": "double"}}""",
        )

        val rollup = Rollup(
            id = "immutable_routing_test",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Test immutable routing",
            sourceIndex = sourceIndex,
            targetIndex = targetIndex,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 1000,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "timestamp", fixedInterval = "1h"),
                Terms(sourceField = "category", targetField = "category"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "value", targetField = "value", metrics = listOf(Sum())),
            ),
            routingField = "category",
        ).let { createRollup(it, it.id) }

        // Try to update with routing_field removed
        val updatedRollup = rollup.copy(
            routingField = null,
            enabled = false,
            jobEnabledTime = null,
        )

        try {
            client().makeRequest(
                "PUT",
                "$ROLLUP_JOBS_BASE_URI/${rollup.id}?if_seq_no=${rollup.seqNo}&if_primary_term=${rollup.primaryTerm}",
                emptyMap(),
                StringEntity(updatedRollup.toJsonString(), ContentType.APPLICATION_JSON),
            )
            fail("Expected 400 error when modifying routing_field")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            assertTrue("Error should mention routing_field", e.message!!.contains("Not allowed to modify"))
        }
    }
}
