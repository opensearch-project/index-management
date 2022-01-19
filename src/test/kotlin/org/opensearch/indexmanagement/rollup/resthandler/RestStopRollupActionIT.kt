/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.rollup.RollupRestTestCase
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestStopRollupActionIT : RollupRestTestCase() {

    @Throws(Exception::class)
    fun `test stopping a started rollup`() {
        val rollup = createRollup(randomRollup().copy(enabled = true, jobEnabledTime = randomInstant(), metadataID = null))
        assertTrue("Rollup was not enabled", rollup.enabled)

        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertFalse("Rollup was not disabled", updatedRollup.enabled)
    }

    @Throws(Exception::class)
    fun `test stopping a stopped rollup`() {
        val rollup = createRollup(randomRollup().copy(enabled = true, jobEnabledTime = randomInstant(), metadataID = null))
        assertTrue("Rollup was not enabled", rollup.enabled)

        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertFalse("Rollup was not disabled", updatedRollup.enabled)

        val secondResponse = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, secondResponse.restStatus())
        val expectedSecondResponse = mapOf("acknowledged" to true)
        assertEquals(expectedSecondResponse, secondResponse.asMap())

        val updatedSecondRollup = getRollup(rollup.id)
        assertFalse("Rollup was not disabled", updatedSecondRollup.enabled)
    }

    @Throws(Exception::class)
    fun `test stopping a finished rollup`() {
        // Create a rollup that finishes
        val rollup = createRollup(
            randomRollup()
                .copy(
                    continuous = false,
                    jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                    enabled = true,
                    jobEnabledTime = Instant.now(),
                    metadataID = null
                )
        )
        createRollupSourceIndex(rollup)
        updateRollupStartTime(rollup)

        // Assert it finished
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup never finished", RollupMetadata.Status.FINISHED, metadata.status)
            // Waiting for job to be disabled here to avoid version conflict exceptions later on
            assertFalse("Job was not disabled", updatedRollup.enabled)
        }

        // Try to stop a finished rollup
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        // Assert it is still in finished status
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup should have stayed finished", RollupMetadata.Status.FINISHED, metadata.status)
        }
    }

    @Throws(Exception::class)
    fun `test stopping a failed rollup`() {
        // Create a rollup that will fail because no source index
        val rollup = randomRollup().copy(
            id = "test_stopping_a_failed_rollup",
            continuous = false,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null
        ).let { createRollup(it, it.id) }
        updateRollupStartTime(rollup)

        // Assert its in failed
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup never failed", RollupMetadata.Status.FAILED, metadata.status)
        }

        // Stop rollup
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        // Assert rollup still failed status
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup should have stayed failed", RollupMetadata.Status.FAILED, metadata.status)
        }
    }

    @Throws(Exception::class)
    fun `test stopping a retry rollup`() {
        // Create a rollup job
        val rollup = createRollup(
            randomRollup()
                .copy(
                    continuous = false,
                    jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                    enabled = true,
                    jobEnabledTime = Instant.now(),
                    metadataID = null
                )
        )

        // Force rollup to execute which should fail as we did not create a source index
        updateRollupStartTime(rollup)

        // Assert rollup is in failed status
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup never failed (no source index)", RollupMetadata.Status.FAILED, metadata.status)
        }

        // Start job to set it into retry status
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_start")
        assertEquals("Start rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        // Assert the job is in retry status
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup is not in RETRY", RollupMetadata.Status.RETRY, metadata.status)
        }

        // Stop the job which is currently in retry status
        val responseTwo = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, responseTwo.restStatus())
        val expectedResponseTwo = mapOf("acknowledged" to true)
        assertEquals(expectedResponseTwo, responseTwo.asMap())

        // Assert the job correctly went back to failed and not stopped
        waitFor {
            val updatedRollup = getRollup(rollup.id)
            val metadata = getRollupMetadata(updatedRollup.metadataID!!)
            assertEquals("Rollup should have stayed finished", RollupMetadata.Status.FAILED, metadata.status)
        }
    }

    @Throws(Exception::class)
    fun `test stopping rollup with metadata`() {
        generateNYCTaxiData("source")
        val rollup = Rollup(
            id = "basic_term_query",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source",
            targetIndex = "target",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = emptyList()
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not STARTED", RollupMetadata.Status.STARTED, rollupMetadata.status)

            // There are two calls to _stop happening serially which is prone to version conflicts during an ongoing job
            // so including it in a waitFor to ensure it can retry a few times
            val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
            assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
            val expectedResponse = mapOf("acknowledged" to true)
            assertEquals(expectedResponse, response.asMap())
        }

        val updatedRollup = getRollup(rollup.id)
        assertFalse("Rollup was not disabled", updatedRollup.enabled)
        val rollupMetadata = getRollupMetadata(updatedRollup.metadataID!!)
        assertEquals("Rollup is not STOPPED", RollupMetadata.Status.STOPPED, rollupMetadata.status)
    }

    @Throws(Exception::class)
    fun `test stop a rollup with no id fails`() {
        try {
            client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI//_stop")
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test stop rollup when multiple shards configured for IM config index`() {
        // setup ism-config index with multiple primary shards
        deleteIndex(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
        val mapping = IndexManagementIndices.indexManagementMappings.trim().trimStart('{').trimEnd('}')
        val settings = Settings.builder()
            .put(INDEX_HIDDEN, true)
            .put(INDEX_NUMBER_OF_SHARDS, 5)
            .build()
        createIndex(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, settings, mapping)
        assertIndexExists(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)

        generateNYCTaxiData("source_multi_shard_stop")
        val rollup = Rollup(
            id = "multi_shard_stop",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_multi_shard_stop",
            targetIndex = "target_multi_shard_stop",
            metadataID = null,
            roles = emptyList(),
            pageSize = 1,
            delay = 0,
            continuous = true,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = emptyList()
        ).let { createRollup(it, it.id) }

        // The updateRollupStartTime call can be missed if the job scheduler hasn't started listening to the new index yet,
        // sleep a bit to let it initialize
        Thread.sleep(2000L)
        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
        }
        val response = client().makeRequest("POST", "$ROLLUP_JOBS_BASE_URI/${rollup.id}/_stop")
        assertEquals("Stop rollup failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedRollup = getRollup(rollup.id)
        assertFalse("Rollup was not disabled", updatedRollup.enabled)
        val rollupMetadata = getRollupMetadataWithRoutingId(rollup.id, updatedRollup.metadataID!!)
        assertEquals("Rollup is not STOPPED", RollupMetadata.Status.STOPPED, rollupMetadata.status)

        // clearing the config index to prevent other tests using this multi shard index
        deleteIndex(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
    }
}
