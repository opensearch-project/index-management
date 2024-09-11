/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.resthandler

import org.opensearch.client.Request
import org.opensearch.client.ResponseException
import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

class RestStartRollupActionIT : RollupRestAPITestCase() {

    private val testName = javaClass.simpleName.lowercase(Locale.ROOT)

    @Throws(Exception::class)
    fun `test starting a stopped rollup`() {
        val rollup = createRollup(randomRollup().copy(enabled = false, jobEnabledTime = null, metadataID = null), rollupId = "$testName-1")
        assertTrue("Rollup was not disabled", !rollup.enabled)

        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertTrue("Rollup was not enabled", updatedRollup.enabled)
    }

    // TODO: With and without metadata
    @Throws(Exception::class)
    fun `test starting a started rollup doesnt change enabled time`() {
        // First create a non-started rollup
        val rollup = createRollup(randomRollup().copy(enabled = false, jobEnabledTime = null, metadataID = null), rollupId = "$testName-2")
        assertTrue("Rollup was not disabled", !rollup.enabled)

        // Enable it to get the job enabled time
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertTrue("Rollup was not enabled", updatedRollup.enabled)

        val secondResponse = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, secondResponse.restStatus())
        val expectedSecondResponse = mapOf("acknowledged" to true)
        assertEquals(expectedSecondResponse, secondResponse.asMap())

        // Confirm the job enabled time is not reset to a newer time if job was already enabled
        val updatedSecondRollup = getRollup(rollup.id)
        assertTrue("Rollup was not enabled", updatedSecondRollup.enabled)
        assertEquals("Jobs had different enabled times", updatedRollup.jobEnabledTime, updatedSecondRollup.jobEnabledTime)
    }

    @Throws(Exception::class)
    fun `test start a rollup with no id fails`() {
        try {
            client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI//_start")
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test starting a failed rollup`() {
        val rollup = Rollup(
            id = "restart_failed_rollup",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_restart_failed_rollup",
            targetIndex = "target_restart_failed_rollup",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = emptyList(),
        ).let { createRollup(it, it.id) }

        // This should fail because we did not create a source index
        updateRollupStartTime(rollup)

        waitFor {
            val updatedRollup = getRollup(rollupId = rollup.id)
            assertNotNull("MetadataID on rollup was null", updatedRollup.metadataID)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            // It should be failed because we did not create the source index
            assertEquals("Status should be failed", RollupMetadata.Status.FAILED, metadata.status)
            assertFalse("Rollup was not disabled", updatedRollup.enabled)
        }

        // Now create the missing source index
        generateNYCTaxiData("source_restart_failed_rollup")

        // And call _start on the failed rollup job
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertTrue("Rollup was not enabled", updatedRollup.enabled)
        waitFor {
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            // It should be in retry now
            assertEquals("Status should be retry", RollupMetadata.Status.RETRY, metadata.status)
        }

        updateRollupStartTime(rollup)

        // Rollup should be able to finished, with actual rolled up docs
        waitFor {
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Status should be finished", RollupMetadata.Status.FINISHED, metadata.status)
            assertEquals("Did not roll up documents", 5000, metadata.stats.documentsProcessed)
            assertTrue("Did not roll up documents", metadata.stats.rollupsIndexed > 0)
        }
    }

    @Throws(Exception::class)
    fun `test starting a finished rollup`() {
        generateNYCTaxiData("source_restart_finished_rollup")
        val rollup = Rollup(
            id = "restart_finished_rollup",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_restart_finished_rollup",
            targetIndex = "target_restart_finished_rollup",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = emptyList(),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)
        var firstRollupsIndexed = 0L
        waitFor {
            val updatedRollup = getRollup(rollupId = rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Status should be finished", RollupMetadata.Status.FINISHED, metadata.status)
            assertEquals("Did not roll up documents", 5000, metadata.stats.documentsProcessed)
            assertTrue("Did not roll up documents", metadata.stats.rollupsIndexed > 0)
            firstRollupsIndexed = metadata.stats.rollupsIndexed
        }

        deleteIndex("target_restart_finished_rollup")

        // And call _start on the failed rollup job
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        updateRollupStartTime(rollup)

        // Rollup should be able to finished, with actual rolled up docs again
        waitFor {
            val updatedRollup = getRollup(rollupId = rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            // logger.info("metadata $metadata")
            assertEquals("Status should be finished", RollupMetadata.Status.FINISHED, metadata.status)
            // Expect 10k docs now (5k from first and 5k from second)
            assertEquals("Did not roll up documents", 10000, metadata.stats.documentsProcessed)
            // Should have twice the rollups indexed now
            assertEquals("Did not index rollup docs", firstRollupsIndexed * 2, metadata.stats.rollupsIndexed)
            assertIndexExists("target_restart_finished_rollup")
        }
    }

    fun `test start rollup when multiple shards configured for IM config index`() {
        // setup ism-config index with multiple primary shards
        val deleteISMIndexRequest = Request("DELETE", "/$INDEX_MANAGEMENT_INDEX")
        adminClient().performRequest(deleteISMIndexRequest)
        val mapping = IndexManagementIndices.indexManagementMappings.trim().trimStart('{').trimEnd('}')
        val settings = Settings.builder()
            .put(INDEX_HIDDEN, true)
            .put(INDEX_NUMBER_OF_SHARDS, 5)
            .build()
        createIndex(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, settings, mapping)
        assertIndexExists(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)

        val rollup = Rollup(
            id = "multi_shard_start",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_multi_shard_start",
            targetIndex = "target_multi_shard_start",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = emptyList(),
        ).let { createRollup(it, it.id) }

        // The updateRollupStartTime call can be missed if the job scheduler hasn't started listening to the new index yet,
        // sleep a bit to let it initialize
        Thread.sleep(2000L)
        updateRollupStartTime(rollup)

        waitFor {
            val updatedRollup = getRollup(rollupId = rollup.id)
            val metadata = getRollupMetadataWithRoutingId(rollup.id, updatedRollup.metadataID!!)
            assertEquals("Status should be failed", RollupMetadata.Status.FAILED, metadata.status)
        }

        val response = client().makeRequest("POST", "${IndexManagementPlugin.ROLLUP_JOBS_BASE_URI}/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertTrue("Rollup was not enabled", updatedRollup.enabled)
        val rollupMetadata = getRollupMetadataWithRoutingId(rollup.id, updatedRollup.metadataID!!)
        assertEquals("Rollup is not RETRY", RollupMetadata.Status.RETRY, rollupMetadata.status)

        // clearing the config index to prevent other tests using this multi shard index
        Thread.sleep(2000L)
        adminClient().performRequest(deleteISMIndexRequest)
        Thread.sleep(2000L)
    }
}
