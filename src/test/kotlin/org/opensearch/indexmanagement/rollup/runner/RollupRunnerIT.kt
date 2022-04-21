/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.runner

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.rollup.RollupRestTestCase
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.RollupMetrics
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.model.metric.ValueCount
import org.opensearch.indexmanagement.rollup.randomCalendarDateHistogram
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_SEARCH_BACKOFF_COUNT
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit

class RollupRunnerIT : RollupRestTestCase() {

    fun `test metadata is created for rollup job when none exists`() {
        val indexName = "test_index_runner_first"

        // Define rollup
        var rollup = randomRollup().copy(
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabledTime = Instant.now(),
            sourceIndex = indexName,
            metadataID = null,
            continuous = false
        )

        // Create source index
        createRollupSourceIndex(rollup)

        // Create rollup job
        rollup = createRollup(rollup = rollup, rollupId = rollup.id)
        assertEquals(indexName, rollup.sourceIndex)
        assertEquals(null, rollup.metadataID)

        // Update rollup start time to run first execution
        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)

            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", rollupMetadata)
            // Non-continuous job will finish in a single execution
            assertEquals("Unexpected metadata state", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }
    }

    fun `test metadata is created for data stream rollup job when none exists`() {
        val dataStreamName = "test-data-stream"

        // Define the rollup job
        var rollup = randomRollup().copy(
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabledTime = Instant.now(),
            sourceIndex = dataStreamName,
            targetIndex = "$dataStreamName-rollup",
            metadataID = null,
            continuous = false
        )

        // Create the source data stream
        client().makeRequest(
            "PUT",
            "/_index_template/test-data-stream-template",
            StringEntity(
                "{ " +
                    "\"index_patterns\": [ \"$dataStreamName\" ], " +
                    "\"data_stream\": { }, " +
                    "\"template\": { \"mappings\": { ${createRollupMappingString(rollup)} } } }",
                ContentType.APPLICATION_JSON
            )
        )
        client().makeRequest("PUT", "/_data_stream/$dataStreamName")

        // Create the rollup job
        rollup = createRollup(rollup = rollup, rollupId = rollup.id)
        assertEquals(dataStreamName, rollup.sourceIndex)
        assertEquals(null, rollup.metadataID)

        // Update the rollup start time to run the first execution
        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)

            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", rollupMetadata)
            // Non-continuous jobs will finish in a single execution
            assertEquals("Unexpected metadata state", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }
        // Delete the data stream
        client().makeRequest("DELETE", "/_data_stream/$dataStreamName")
    }

    fun `test metadata set to failed when rollup job has a metadata id but metadata doc doesn't exist`() {
        val indexName = "test_index_runner_second"

        // Define rollup
        var rollup = randomRollup().copy(
            id = "metadata_set_failed_id_doc_not_exist",
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabledTime = Instant.now(),
            sourceIndex = indexName,
            targetIndex = "${indexName}_target",
            metadataID = null,
            continuous = true
        )

        // Create source index
        createRollupSourceIndex(rollup)

        // Add a document using the rollup's DateHistogram source field to ensure a metadata document is created
        putDateDocumentInSourceIndex(rollup)

        // Create rollup job
        rollup = createRollup(rollup = rollup, rollupId = rollup.id)
        assertEquals(indexName, rollup.sourceIndex)
        assertEquals(null, rollup.metadataID)

        // Update rollup start time to run first execution
        updateRollupStartTime(rollup)

        var previousRollupMetadata: RollupMetadata? = null
        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)

            previousRollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", previousRollupMetadata)
            assertEquals("Unexpected metadata status", RollupMetadata.Status.INIT, previousRollupMetadata!!.status)
        }

        // Delete rollup metadata
        assertNotNull("Previous rollup metadata was not saved", previousRollupMetadata)
        deleteRollupMetadata(previousRollupMetadata!!.id)

        // Update rollup start time to run second execution
        updateRollupStartTime(rollup)

        waitFor() {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            assertNotEquals("Rollup job metadata was not changed", previousRollupMetadata!!.id, rollupJob.metadataID)

            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", rollupMetadata)
            assertEquals("Unexpected metadata state", RollupMetadata.Status.FAILED, rollupMetadata.status)
        }

        // TODO: Call _start to retry and test recovery behavior
    }

    // NOTE: The test document being added for creating the start/end windows has the timestamp of Instant.now().
    // It's possible that this timestamp can fall on the very edge of the endtime and therefore execute the second time around
    // which could result in this test failing.
    // Setting the interval to something large to minimize this scenario.
    fun `test no-op execution when a full window of time to rollup is not available`() {
        val indexName = "test_index_runner_third"

        // Define rollup
        var rollup = randomRollup().copy(
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabledTime = Instant.now(),
            sourceIndex = indexName,
            metadataID = null,
            continuous = true,
            dimensions = listOf(
                randomCalendarDateHistogram().copy(
                    calendarInterval = "1y"
                )
            )
        )

        // Create source index
        createRollupSourceIndex(rollup)

        // Add a document using the rollup's DateHistogram source field to ensure a metadata document is created
        putDateDocumentInSourceIndex(rollup)

        // Create rollup job
        rollup = createRollup(rollup = rollup, rollupId = rollup.id)
        assertEquals(indexName, rollup.sourceIndex)
        assertEquals(null, rollup.metadataID)

        // Update rollup start time to run first execution
        updateRollupStartTime(rollup)

        var previousRollupMetadata: RollupMetadata? = null
        // Assert on first execution
        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)

            previousRollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", previousRollupMetadata)
            assertEquals("Unexpected metadata status", RollupMetadata.Status.INIT, previousRollupMetadata!!.status)
        }

        assertNotNull("Previous rollup metadata was not saved", previousRollupMetadata)

        // Update rollup start time to run second execution
        updateRollupStartTime(rollup)

        // Wait some arbitrary amount of time so the execution happens
        // Not using waitFor since this is testing a lack of state change
        Thread.sleep(10000)

        // Assert that no changes were made
        val currentMetadata = getRollupMetadata(previousRollupMetadata!!.id)
        assertEquals("Rollup metadata was updated", previousRollupMetadata!!.lastUpdatedTime, currentMetadata.lastUpdatedTime)
    }

    fun `test running job with no source index fails`() {
        val indexName = "test_index_runner_fourth"

        // Define rollup
        var rollup = randomRollup().copy(
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabledTime = Instant.now(),
            sourceIndex = indexName,
            metadataID = null,
            continuous = true
        )

        // Create rollup job
        rollup = createRollup(rollup = rollup, rollupId = rollup.id)
        assertEquals(indexName, rollup.sourceIndex)
        assertEquals(null, rollup.metadataID)

        // Update rollup start time to run first execution
        updateRollupStartTime(rollup)

        var rollupMetadata: RollupMetadata?
        // Assert on first execution
        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job not found", rollupJob)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)

            rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", rollupMetadata)
            assertEquals("Unexpected metadata status", RollupMetadata.Status.FAILED, rollupMetadata!!.status)
            assertEquals("Unexpected failure reason", "No indices found for [${rollup.sourceIndex}]", rollupMetadata!!.failureReason)
        }

        // TODO: Call _start to retry and test recovery behavior?
    }

    fun `test metadata stats contains correct info`() {
        // TODO: we are setting these jobs serially since we know concurrently running jobs can cause failures to update metadata sometimes.

        generateNYCTaxiData("source_runner_fifth")

        val rollup = Rollup(
            id = "basic_stats_check_runner_fifth",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic stats test",
            sourceIndex = "source_runner_fifth",
            targetIndex = "target_runner_fifth",
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        val secondRollup = Rollup(
            id = "all_inclusive_intervals_runner_fifth",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic stats test",
            sourceIndex = "source_runner_fifth",
            targetIndex = "target_runner_fifth",
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "100d")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        val thirdRollup = Rollup(
            id = "second_interval_runner_fifth",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic 1s test",
            sourceIndex = "source_runner_fifth",
            targetIndex = "target_runner_fifth",
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex)) }

        val finishedRollup = waitFor() {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
            rollupJob
        }

        updateRollupStartTime(secondRollup)

        val secondFinishedRollup = waitFor() {
            val rollupJob = getRollup(rollupId = secondRollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
            rollupJob
        }

        updateRollupStartTime(thirdRollup)

        val thirdFinishedRollup = waitFor() {
            val rollupJob = getRollup(rollupId = thirdRollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished $rollupMetadata", RollupMetadata.Status.FINISHED, rollupMetadata.status)
            rollupJob
        }

        refreshAllIndices()

        val rollupMetadataID = finishedRollup.metadataID!!
        val rollupMetadata = getRollupMetadata(rollupMetadataID)
        val secondRollupMetadataID = secondFinishedRollup.metadataID!!
        val secondRollupMetadata = getRollupMetadata(secondRollupMetadataID)
        val thirdRollupMetadataID = thirdFinishedRollup.metadataID!!
        val thirdRollupMetadata = getRollupMetadata(thirdRollupMetadataID)

        // These might seem like magic numbers but they are static/fixed based off the dataset in the resources
        // We have two pages processed because afterKey is always returned if there is data in the response
        // So the first pagination returns an afterKey and the second doesn't
        assertEquals("Did not have 2 pages processed", 2L, rollupMetadata.stats.pagesProcessed)
        // This is a non-continuous job that rolls up every document of which there are 5k
        assertEquals("Did not have 5000 documents processed", 5000L, rollupMetadata.stats.documentsProcessed)
        // Based on the very first document using the tpep_pickup_datetime date field and an hourly rollup there
        // should be 10 buckets with data in them which means 10 rollup documents
        assertEquals("Did not have 10 rollups indexed", 10L, rollupMetadata.stats.rollupsIndexed)
        // These are hard to test.. just assert they are more than 0
        assertTrue("Did not spend time indexing", rollupMetadata.stats.indexTimeInMillis > 0L)
        assertTrue("Did not spend time searching", rollupMetadata.stats.searchTimeInMillis > 0L)

        assertEquals("Did not have 2 pages processed", 2L, secondRollupMetadata.stats.pagesProcessed)
        // This is a non-continuous job that rolls up every document of which there are 5k
        assertEquals("Did not have 5000 documents processed", 5000L, secondRollupMetadata.stats.documentsProcessed)
        // Based on the very first document using the tpep_pickup_datetime date field and a 100 day rollup there
        // should be 1 bucket with data in them which means 1 rollup documents
        assertEquals("Did not have 1 rollup indexed", 1L, secondRollupMetadata.stats.rollupsIndexed)
        // These are hard to test.. just assert they are more than 0
        assertTrue("Did not spend time indexing", secondRollupMetadata.stats.indexTimeInMillis > 0L)
        assertTrue("Did not spend time searching", secondRollupMetadata.stats.searchTimeInMillis > 0L)

        assertEquals("Did not have 28 pages processed", 28L, thirdRollupMetadata.stats.pagesProcessed)
        // This is a non-continuous job that rolls up every document of which there are 5k
        assertEquals("Did not have 5000 documents processed", 5000L, thirdRollupMetadata.stats.documentsProcessed)
        // Based on the very first document using the tpep_pickup_datetime date field and a 1 second rollup there
        // should be 2667 buckets with data in them which means 2667 rollup documents
        assertEquals("Did not have 2667 rollups indexed", 2667L, thirdRollupMetadata.stats.rollupsIndexed)
        // These are hard to test.. just assert they are more than 0
        assertTrue("Did not spend time indexing", thirdRollupMetadata.stats.indexTimeInMillis > 0L)
        assertTrue("Did not spend time searching", thirdRollupMetadata.stats.searchTimeInMillis > 0L)
    }

    fun `test changing page size during execution`() {
        // The idea with this test is we set the original pageSize=1 and fixedInterval to 1s to take a long time
        // to rollup a single document per execution which gives us enough time to change the pageSize to something large
        generateNYCTaxiData("source_runner_sixth")

        val rollup = Rollup(
            id = "page_size_runner_sixth",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic change of page size",
            sourceIndex = "source_runner_sixth",
            targetIndex = "target_runner_sixth",
            metadataID = null,
            roles = emptyList(),
            pageSize = 1,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex)) }

        val startedRollup = waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not started", RollupMetadata.Status.STARTED, rollupMetadata.status)
            rollupJob
        }

        client().makeRequest(
            "PUT",
            "$ROLLUP_JOBS_BASE_URI/${startedRollup.id}?if_seq_no=${startedRollup.seqNo}&if_primary_term=${startedRollup.primaryTerm}",
            emptyMap(), rollup.copy(pageSize = 1000).toHttpEntity()
        )

        val finishedRollup = waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not started", RollupMetadata.Status.FINISHED, rollupMetadata.status)
            rollupJob
        }

        val rollupMetadataID = finishedRollup.metadataID!!
        val rollupMetadata = getRollupMetadata(rollupMetadataID)

        // Randomly choosing 100.. if it didn't work we'd either fail hitting the timeout in waitFor or we'd have thousands of pages processed
        assertTrue("Did not have less than 100 pages processed", rollupMetadata.stats.pagesProcessed < 100L)
    }

    fun `test search max buckets breaker`() {
        generateNYCTaxiData("source_runner_seventh")
        // Set the search max buckets to 50 and rollup search retry count to 0 so it won't retry on failure. This is to confirm first that yes we do get an error and moved into failed state.
        client().makeRequest("PUT", "/_cluster/settings", StringEntity("""{"persistent":{"search.max_buckets":"50", "${ROLLUP_SEARCH_BACKOFF_COUNT.key}": 0 }}""", ContentType.APPLICATION_JSON))

        val rollup = Rollup(
            id = "page_size_no_retry_first_runner_seventh",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic page size",
            sourceIndex = "source_runner_seventh",
            targetIndex = "target_runner_seventh",
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1m")),
            metrics = listOf(RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())))
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex)) }

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not failed", RollupMetadata.Status.FAILED, rollupMetadata.status)
            assertTrue("Did not contain failure message about too many buckets", rollupMetadata.failureReason?.contains("Trying to create too many buckets") == true)
        }

        // If we get to this point it means that yes it does fail with too many buckets error, now we'll try with backoff and having it reduce below the max buckets limit

        client().makeRequest("PUT", "/_cluster/settings", StringEntity("""{"persistent":{"search.max_buckets":"50", "${ROLLUP_SEARCH_BACKOFF_COUNT.key}": 5 }}""", ContentType.APPLICATION_JSON))

        val secondRollup = Rollup(
            id = "page_size_with_retry_second_runner_seventh",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic page size",
            sourceIndex = "source_runner_seventh",
            targetIndex = "new_target_runner_seventh",
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1m")),
            metrics = listOf(RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())))
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(secondRollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(secondRollup.targetIndex)) }

        waitFor {
            val rollupJob = getRollup(rollupId = secondRollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
            assertNull("Had a failure reason", rollupMetadata.failureReason)
        }
    }

    // Tests that a continuous rollup will not be processed until the end of the interval plus delay passes
    fun `test delaying continuous execution`() {
        val indexName = "test_index_runner_eighth"
        val delay: Long = 15000
        // Define rollup
        var rollup = randomRollup().copy(
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobEnabledTime = Instant.now(),
            sourceIndex = indexName,
            metadataID = null,
            continuous = true,
            delay = delay,
            dimensions = listOf(
                randomCalendarDateHistogram().copy(
                    calendarInterval = "5s"
                )
            )
        )

        // Create source index
        createRollupSourceIndex(rollup)
        // Add a document using the rollup's DateHistogram source field to ensure a metadata document is created
        putDateDocumentInSourceIndex(rollup)

        // Create rollup job
        rollup = createRollup(rollup = rollup, rollupId = rollup.id)

        var nextExecutionTime = rollup.schedule.getNextExecutionTime(null).toEpochMilli()
        val expectedExecutionTime = rollup.jobEnabledTime!!.plusMillis(delay).toEpochMilli()
        val delayIsCorrect = ((expectedExecutionTime - nextExecutionTime) > -500) && ((expectedExecutionTime - nextExecutionTime) < 500)
        assertTrue("Delay was not correctly applied", delayIsCorrect)

        waitFor {
            // Wait until half a second before the intended execution time
            assertTrue(Instant.now().toEpochMilli() >= nextExecutionTime - 500)
            // Still should not have run at this point
            assertFalse("Target rollup index was created before the delay should allow", indexExists(rollup.targetIndex))
        }
        val rollupMetadata = waitFor {
            assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex))
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", rollupMetadata)
            rollupMetadata
        }
        nextExecutionTime = rollup.schedule.getNextExecutionTime(null).toEpochMilli()
        val nextExecutionOffset = (nextExecutionTime - Instant.now().toEpochMilli()) - 60000
        val nextExecutionIsCorrect = nextExecutionOffset < 5000 && nextExecutionOffset > -5000
        assertTrue("Next execution time not updated correctly", nextExecutionIsCorrect)
        val nextWindowStartTime: Instant = rollupMetadata.continuous!!.nextWindowStartTime
        val nextWindowEndTime: Instant = rollupMetadata.continuous!!.nextWindowEndTime
        // Assert that after the window was updated, it falls approximately around 'now'
        assertTrue("Rollup window start time is incorrect", nextWindowStartTime.plusMillis(delay).minusMillis(1000) < Instant.now())
        assertTrue("Rollup window end time is incorrect", nextWindowEndTime.plusMillis(delay).plusMillis(1000) > Instant.now())

        // window length should be 5 seconds
        val expectedWindowEnd = nextWindowStartTime.plusMillis(5000)
        assertEquals("Rollup window length applied incorrectly", expectedWindowEnd, nextWindowEndTime)
    }

    fun `test non continuous delay does nothing`() {
        generateNYCTaxiData("source_runner_ninth")

        // Setting the delay to this time so most of the data records would be excluded if delay were applied
        val goalDateMS: Long = Instant.parse("2018-11-30T00:00:00Z").toEpochMilli()
        val testDelay: Long = Instant.now().toEpochMilli() - goalDateMS
        val rollup = Rollup(
            id = "non_continuous_delay_stats_check",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic delay test",
            sourceIndex = "source_runner_ninth",
            targetIndex = "target_runner_ninth",
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = testDelay,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        val now = Instant.now()
        val intervalMillis = (rollup.schedule as IntervalSchedule).interval * 60 * 1000
        val nextExecutionTime = rollup.schedule.getNextExecutionTime(now).toEpochMilli()
        val remainder = intervalMillis - ((now.toEpochMilli() - rollup.jobEnabledTime!!.toEpochMilli()) % intervalMillis)
        val expectedExecutionTime = now.toEpochMilli() + remainder
        val delayIsCorrect = ((expectedExecutionTime - nextExecutionTime) > -500) && ((expectedExecutionTime - nextExecutionTime) < 500)
        assertTrue("Non continuous execution time was not correct", delayIsCorrect)

        updateRollupStartTime(rollup)

        val finishedRollup = waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished $rollupMetadata", RollupMetadata.Status.FINISHED, rollupMetadata.status)
            rollupJob
        }

        refreshAllIndices()

        // No data should be excluded as the delay should not have been included
        val rollupMetadataID = finishedRollup.metadataID!!
        val rollupMetadata = getRollupMetadata(rollupMetadataID)
        // These values would not match up with a delay
        assertEquals("Did not have 2 pages processed", 2L, rollupMetadata.stats.pagesProcessed)
        // This is a non-continuous job that rolls up every document of which there are 5k
        assertEquals("Did not have 5000 documents processed", 5000L, rollupMetadata.stats.documentsProcessed)
        // Based on the very first document using the tpep_pickup_datetime date field and an hourly rollup there
        // should be 10 buckets with data in them which means 10 rollup documents
        assertEquals("Did not have 10 rollups indexed", 10L, rollupMetadata.stats.rollupsIndexed)
        // These are hard to test.. just assert they are more than 0
        assertTrue("Did not spend time indexing", rollupMetadata.stats.indexTimeInMillis > 0L)
        assertTrue("Did not spend time searching", rollupMetadata.stats.searchTimeInMillis > 0L)
    }

    // Tests that the continuous delay excludes recent data correctly
    fun `test continuous delay exclusion period`() {
        generateNYCTaxiData("source_runner_tenth")

        // Setting the delay to this time so most of the data records are excluded
        val goalDateMS: Long = Instant.parse("2018-11-30T00:00:00Z").toEpochMilli()
        val testDelay: Long = Instant.now().toEpochMilli() - goalDateMS
        val rollup = Rollup(
            id = "continuous_delay_stats_check",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic delay test",
            sourceIndex = "source_runner_tenth",
            targetIndex = "target_runner_tenth",
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = testDelay,
            continuous = true,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup, Instant.now().minusMillis(testDelay).minusMillis(55000).toEpochMilli())

        val finishedRollup = waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not started $rollupMetadata", RollupMetadata.Status.STARTED, rollupMetadata.status)
            assertTrue("Continuous rollup did not process history", rollupMetadata.continuous!!.nextWindowEndTime.toEpochMilli() > goalDateMS)
            rollupJob
        }

        refreshAllIndices()

        val rollupMetadataID = finishedRollup.metadataID!!
        val rollupMetadata = getRollupMetadata(rollupMetadataID)
        // These numbers seem arbitrary, but match the case when the continuous rollup stops processing at 2018-11-30
        assertEquals("Did not have 35 pages processed", 35, rollupMetadata.stats.pagesProcessed)
        // This is a continuous job that rolls up documents before 2018-11-30, of which there are 4
        assertEquals("Did not have 4 documents processed", 4, rollupMetadata.stats.documentsProcessed)
        // Based on the very first document using the tpep_pickup_datetime date field and a 1 hour rollup there
        // should be 2 buckets with data in them which means 2 rollup documents
        assertEquals("Did not have 2 rollups indexed", 2, rollupMetadata.stats.rollupsIndexed)
        // These are hard to test.. just assert they are more than 0
        assertTrue("Did not spend time indexing", rollupMetadata.stats.indexTimeInMillis > 0L)
        assertTrue("Did not spend time searching", rollupMetadata.stats.searchTimeInMillis > 0L)
    }

    // TODO: Test scenarios:
    // - Source index deleted after first execution
    //      * If this is with a source index pattern and the underlying indices are recreated but with different data
    //        what would the behavior be? Restarting the rollup would cause there to be different data for the previous windows
    // - Invalid source index mappings
    // - Target index deleted after first execution
    // - Source index with pattern
    // - Source index with pattern with invalid indices
    // - Source index with pattern mapping to some closed indices

    private fun deleteRollupMetadata(metadataId: String) {
        val response = client().makeRequest("DELETE", "${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}/_doc/$metadataId")
        assertEquals("Unable to delete rollup metadata $metadataId", RestStatus.OK, response.restStatus())
    }
}
