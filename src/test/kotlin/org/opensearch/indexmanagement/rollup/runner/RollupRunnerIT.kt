/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.runner

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.engine.EngineConfig
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_REPLICAS
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
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
import org.opensearch.rest.RestRequest
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Collections.emptyMap
import java.util.Locale

class RollupRunnerIT : RollupRestTestCase() {
    private val testName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test metadata is created for rollup job when none exists`() {
        val indexName = "test_index_runner_first"

        // Define rollup
        var rollup =
            randomRollup().copy(
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobEnabledTime = Instant.now(),
                sourceIndex = indexName,
                metadataID = null,
                continuous = false,
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

    @Suppress("UNCHECKED_CAST")
    fun `test rollup with creating target index with specific settings`() {
        val sourceIdxTestName = "source_idx_test_settings"
        val targetIdxTestName = "target_idx_test_settings"
        val targetIndexReplicas = 0
        val targetIndexCodec = "best_compression"
        generateNYCTaxiData(sourceIdxTestName)

        val rollup =
            Rollup(
                id = testName,
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic stats test",
                sourceIndex = sourceIdxTestName,
                targetIndex = targetIdxTestName,
                targetIndexSettings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, targetIndexReplicas)
                    .put(EngineConfig.INDEX_CODEC_SETTING.key, targetIndexCodec)
                    .build(),
                metadataID = null,
                roles = emptyList(),
                pageSize = 100,
                delay = 0,
                continuous = false,
                dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
                metrics =
                listOf(
                    RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Average())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex)) }

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)

            val rawRes = client().makeRequest(RestRequest.Method.GET.name, "/$targetIdxTestName/_settings", mapOf("flat_settings" to "true"))
            assertTrue(rawRes.restStatus() == RestStatus.OK)
            val indexSettingsRes = rawRes.asMap()[targetIdxTestName] as Map<String, Map<String, Any>>
            val settingsRes = indexSettingsRes["settings"]
            assertNotNull("Rollup index did not have any settings", settingsRes)
            assertEquals(
                "Rollup index did not have correct codec setting",
                targetIndexCodec,
                settingsRes?.getValue(EngineConfig.INDEX_CODEC_SETTING.key),
            )
            assertEquals(
                "Rollup index did not have correct replicas setting",
                targetIndexReplicas.toString(),
                settingsRes?.getValue(IndexMetadata.SETTING_NUMBER_OF_REPLICAS),
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test rollup with avg metric`() {
        val sourceIdxTestName = "source_idx_test"
        val targetIdxTestName = "target_idx_test"
        val propertyName = "passenger_count"
        val avgMetricName = "avg_passenger_count"

        generateNYCTaxiData(sourceIdxTestName)

        val rollup =
            Rollup(
                id = "rollup_test",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic stats test",
                sourceIndex = sourceIdxTestName,
                targetIndex = targetIdxTestName,
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 100,
                delay = 0,
                continuous = false,
                dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
                metrics =
                listOf(
                    RollupMetrics(sourceField = propertyName, targetField = propertyName, metrics = listOf(Average())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex)) }

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)

            // Term query
            val req =
                """
                {
                    "size": 0,
                    "query": {
                      "match_all": {}
                    },
                    "aggs": {
                        "$avgMetricName": {
                            "avg": {
                                "field": "$propertyName"
                            }
                        }
                    }
                }
                """.trimIndent()
            var rawRes = client().makeRequest(RestRequest.Method.POST.name, "/$sourceIdxTestName/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            assertTrue(rawRes.restStatus() == RestStatus.OK)
            var rollupRes = client().makeRequest(RestRequest.Method.POST.name, "/$targetIdxTestName/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            assertTrue(rollupRes.restStatus() == RestStatus.OK)
            var rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
            var rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
            assertEquals(
                "Source and rollup index did not return same avg results",
                rawAggRes.getValue(avgMetricName)["value"],
                rollupAggRes.getValue(avgMetricName)["value"],
            )
        }
    }

    fun `test metadata is created for data stream rollup job when none exists`() {
        val dataStreamName = "test-data-stream"

        // Define the rollup job
        var rollup =
            randomRollup().copy(
                id = "$testName-1",
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobEnabledTime = Instant.now(),
                sourceIndex = dataStreamName,
                targetIndex = "$dataStreamName-rollup",
                metadataID = null,
                continuous = false,
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
                ContentType.APPLICATION_JSON,
            ),
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
        var rollup =
            randomRollup().copy(
                id = "metadata_set_failed_id_doc_not_exist",
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobEnabledTime = Instant.now(),
                sourceIndex = indexName,
                targetIndex = "${indexName}_target",
                metadataID = null,
                continuous = false,
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
        rollup =
            waitFor {
                val rollupJob = getRollup(rollupId = rollup.id)
                assertNotNull("Rollup job not found", rollupJob)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                assertFalse("Rollup job is still enabled", rollupJob.enabled)

                previousRollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                assertNotNull("Rollup metadata not found", previousRollupMetadata)
                assertEquals("Unexpected metadata status", RollupMetadata.Status.FINISHED, previousRollupMetadata!!.status)
                rollupJob
            }
        // Delete rollup metadata
        assertNotNull("Previous rollup metadata was not saved", previousRollupMetadata)
        deleteRollupMetadata(previousRollupMetadata!!.id)

        // Enable rollup and Update start time to run second execution
        client().makeRequest(
            "PUT",
            "$ROLLUP_JOBS_BASE_URI/${rollup.id}?if_seq_no=${rollup.seqNo}&if_primary_term=${rollup.primaryTerm}",
            emptyMap(), rollup.copy(enabled = true, jobEnabledTime = Instant.now()).toHttpEntity(),
        )

        updateRollupStartTime(rollup)

        waitFor {
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
        var rollup =
            randomRollup().copy(
                id = "$testName-2",
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobEnabledTime = Instant.now(),
                sourceIndex = indexName,
                metadataID = null,
                continuous = true,
                dimensions =
                listOf(
                    randomCalendarDateHistogram().copy(
                        calendarInterval = "1y",
                    ),
                ),
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
        var rollup =
            randomRollup().copy(
                id = "$testName-3",
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobEnabledTime = Instant.now(),
                sourceIndex = indexName,
                metadataID = null,
                continuous = true,
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

        val rollup =
            Rollup(
                id = "basic_stats_check_runner_fifth",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic stats test",
                sourceIndex = "source_runner_fifth",
                targetIndex = "target_runner_fifth",
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 100,
                delay = 0,
                continuous = false,
                dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
                metrics =
                listOf(
                    RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())),
                ),
            ).let { createRollup(it, it.id) }

        val secondRollup =
            Rollup(
                id = "all_inclusive_intervals_runner_fifth",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic stats test",
                sourceIndex = "source_runner_fifth",
                targetIndex = "target_runner_fifth",
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 100,
                delay = 0,
                continuous = false,
                dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "100d")),
                metrics =
                listOf(
                    RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())),
                ),
            ).let { createRollup(it, it.id) }

        val thirdRollup =
            Rollup(
                id = "second_interval_runner_fifth",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic 1s test",
                sourceIndex = "source_runner_fifth",
                targetIndex = "target_runner_fifth",
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 100,
                delay = 0,
                continuous = false,
                dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s")),
                metrics =
                listOf(
                    RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex)) }

        val finishedRollup =
            waitFor {
                val rollupJob = getRollup(rollupId = rollup.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
                rollupJob
            }

        updateRollupStartTime(secondRollup)

        val secondFinishedRollup =
            waitFor {
                val rollupJob = getRollup(rollupId = secondRollup.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
                rollupJob
            }

        updateRollupStartTime(thirdRollup)

        val thirdFinishedRollup =
            waitFor {
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

        val rollup =
            Rollup(
                id = "page_size_runner_sixth",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic change of page size",
                sourceIndex = "source_runner_sixth",
                targetIndex = "target_runner_sixth",
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 1,
                delay = 0,
                continuous = false,
                dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s")),
                metrics =
                listOf(
                    RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex)) }

        val startedRollup =
            waitFor {
                val rollupJob = getRollup(rollupId = rollup.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not started", RollupMetadata.Status.STARTED, rollupMetadata.status)
                rollupJob
            }

        client().makeRequest(
            "PUT",
            "$ROLLUP_JOBS_BASE_URI/${startedRollup.id}?if_seq_no=${startedRollup.seqNo}&if_primary_term=${startedRollup.primaryTerm}",
            emptyMap(), rollup.copy(pageSize = 1000).toHttpEntity(),
        )

        val finishedRollup =
            waitFor {
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

        val rollup =
            Rollup(
                id = "page_size_no_retry_first_runner_seventh",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic page size",
                sourceIndex = "source_runner_seventh",
                targetIndex = "target_runner_seventh",
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 100,
                delay = 0,
                continuous = false,
                dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1m")),
                metrics = listOf(RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))),
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

        val secondRollup =
            Rollup(
                id = "page_size_with_retry_second_runner_seventh",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic page size",
                sourceIndex = "source_runner_seventh",
                targetIndex = "new_target_runner_seventh",
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 100,
                delay = 0,
                continuous = false,
                dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1m")),
                metrics = listOf(RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))),
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
        val delay: Long = 7_500
        // Define rollup
        var rollup =
            randomRollup().copy(
                id = "$testName-4",
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobEnabledTime = Instant.now(),
                sourceIndex = indexName,
                metadataID = null,
                continuous = true,
                delay = delay,
                dimensions =
                listOf(
                    randomCalendarDateHistogram().copy(
                        calendarInterval = "5s",
                    ),
                ),
            )

        // Create source index
        createRollupSourceIndex(rollup)
        // Add a document using the rollup's DateHistogram source field to ensure a metadata document is created
        putDateDocumentInSourceIndex(rollup)

        // Create rollup job
        val jobStartTime = Instant.now()
        val rollupNow =
            rollup.copy(
                jobSchedule = IntervalSchedule(jobStartTime, 1, ChronoUnit.MINUTES),
                jobEnabledTime = jobStartTime,
            )
        rollup = createRollup(rollup = rollupNow, rollupId = rollupNow.id)

        val expectedFirstExecutionTime = rollup.jobSchedule.getNextExecutionTime(null).toEpochMilli()
        assertTrue("The first job execution time should be equal [job start time] + [delay].", expectedFirstExecutionTime == jobStartTime.toEpochMilli() + delay)

        waitFor {
            assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex))
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Rollup metadata not found", rollupMetadata)
        }

        val now = Instant.now().toEpochMilli()
        assertTrue("The first job execution must happen after [job start time] + [delay]", now > jobStartTime.toEpochMilli() + delay)

        val secondExecutionTime = rollup.schedule.getNextExecutionTime(null).toEpochMilli()
        assertTrue("The second job execution time should be not earlier than a minute after the first execution.", secondExecutionTime - expectedFirstExecutionTime == 60_000L)
    }

    fun `test non continuous delay does nothing`() {
        generateNYCTaxiData("source_runner_ninth")

        // Setting the delay to this time so most of the data records would be excluded if delay were applied
        val goalDateMS: Long = Instant.parse("2018-11-30T00:00:00Z").toEpochMilli()
        val testDelay: Long = Instant.now().toEpochMilli() - goalDateMS
        val rollup =
            Rollup(
                id = "non_continuous_delay_stats_check",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic delay test",
                sourceIndex = "source_runner_ninth",
                targetIndex = "target_runner_ninth",
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 100,
                delay = testDelay,
                continuous = false,
                dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
                metrics =
                listOf(
                    RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())),
                ),
            ).let { createRollup(it, it.id) }

        val now = Instant.now()
        val intervalMillis = (rollup.schedule as IntervalSchedule).interval * 60 * 1000
        val nextExecutionTime = rollup.schedule.getNextExecutionTime(now).toEpochMilli()
        val remainder = intervalMillis - ((now.toEpochMilli() - rollup.jobEnabledTime!!.toEpochMilli()) % intervalMillis)
        val expectedExecutionTime = now.toEpochMilli() + remainder
        val delayIsCorrect = ((expectedExecutionTime - nextExecutionTime) > -500) && ((expectedExecutionTime - nextExecutionTime) < 500)
        assertTrue("Non continuous execution time was not correct", delayIsCorrect)

        updateRollupStartTime(rollup)

        val finishedRollup =
            waitFor {
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
        val rollup =
            Rollup(
                id = "continuous_delay_stats_check",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic delay test",
                sourceIndex = "source_runner_tenth",
                targetIndex = "target_runner_tenth",
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 100,
                delay = testDelay,
                continuous = true,
                dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
                metrics =
                listOf(
                    RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup, Instant.now().minusMillis(testDelay).minusMillis(55000).toEpochMilli())

        val finishedRollup =
            waitFor {
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
        // In some cases it seems that these times are less than 1ms - which causes fails on ubuntu instances (at least that was detected)
        assertTrue("Did not spend time searching", rollupMetadata.stats.searchTimeInMillis >= 0L)
    }

    fun `test rollup action with alias as target_index successfully`() {
        generateNYCTaxiData("source_runner_sixth_eleventh_1")

        // Create index with alias, without mappings
        val indexAlias = "alias_as_target_index"
        val backingIndex = "backing_target_index"
        val builtSettings =
            Settings.builder().let {
                it.put(INDEX_NUMBER_OF_REPLICAS, "1")
                it.put(INDEX_NUMBER_OF_SHARDS, "1")
                it
            }.build()
        val aliases = "\"$indexAlias\": { \"is_write_index\": true }"
        createIndex(backingIndex, builtSettings, null, aliases)

        refreshAllIndices()

        val rollup =
            Rollup(
                id = "runner_with_alias_as_target",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic change of page size",
                sourceIndex = "source_runner_sixth_eleventh_1",
                targetIndex = indexAlias,
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 1000,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()),
                    ),
                ),
            ).let { createRollup(it, it.id) }

        // First run, backing index is empty: no mappings, no rollup_index setting, no rollupjobs in _META
        updateRollupStartTime(rollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(backingIndex)) }

        var startedRollup =
            waitFor {
                val rollupJob = getRollup(rollupId = rollup.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
                assertTrue("Rollup is not disabled", !rollupJob.enabled)
                rollupJob
            }
        var rollupMetadataID = startedRollup.metadataID!!
        var rollupMetadata = getRollupMetadata(rollupMetadataID)
        assertTrue("Did not process any doc during rollup", rollupMetadata.stats.documentsProcessed > 0)

        // restart job
        client().makeRequest(
            "PUT",
            "$ROLLUP_JOBS_BASE_URI/${startedRollup.id}?if_seq_no=${startedRollup.seqNo}&if_primary_term=${startedRollup.primaryTerm}",
            emptyMap(), rollup.copy(enabled = true).toHttpEntity(),
        )
        // Second run, backing index is setup just like any other rollup index
        updateRollupStartTime(rollup)

        startedRollup =
            waitFor {
                val rollupJob = getRollup(rollupId = rollup.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata1 = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata1.status)
                rollupJob
            }

        rollupMetadataID = startedRollup.metadataID!!
        rollupMetadata = getRollupMetadata(rollupMetadataID)

        assertTrue("Did not process any doc during rollup", rollupMetadata.stats.documentsProcessed > 0)
    }

    fun `test rollup action with alias as target_index with multiple backing indices successfully`() {
        generateNYCTaxiData("source_runner_sixth_29932")

        // Create index with alias, without mappings
        val indexAlias = "alias_as_target_index_2"
        val backingIndex1 = "backing_target_index1-000001"
        val backingIndex2 = "backing_target_index1-000002"
        val builtSettings =
            Settings.builder().let {
                it.put(INDEX_NUMBER_OF_REPLICAS, "1")
                it.put(INDEX_NUMBER_OF_SHARDS, "1")
                it
            }.build()
        val aliases = "\"$indexAlias\": { \"is_write_index\": true }"
        createIndex(backingIndex1, builtSettings, null, aliases)

        refreshAllIndices()

        val rollup =
            Rollup(
                id = "page_size_runner_sixth_2",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic change of page size",
                sourceIndex = "source_runner_sixth_29932",
                targetIndex = indexAlias,
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 1000,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()),
                    ),
                ),
            ).let { createRollup(it, it.id) }

        // First run, backing index is empty: no mappings, no rollup_index setting, no rollupjobs in _META
        updateRollupStartTime(rollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(backingIndex1)) }

        var startedRollup =
            waitFor {
                val rollupJob = getRollup(rollupId = rollup.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
                assertTrue("Rollup is not disabled", !rollupJob.enabled)
                rollupJob
            }
        var rollupMetadataID = startedRollup.metadataID!!
        var rollupMetadata = getRollupMetadata(rollupMetadataID)
        assertTrue("Did not process any doc during rollup", rollupMetadata.stats.documentsProcessed > 0)

        // do rollover on alias
        val rolloverResponse = client().makeRequest("POST", "/$indexAlias/_rollover")
        assertEquals(RestStatus.OK, rolloverResponse.restStatus())
        waitFor { assertTrue("index was not created after rollover", indexExists(backingIndex2)) }

        // restart job
        client().makeRequest(
            "PUT",
            "$ROLLUP_JOBS_BASE_URI/${startedRollup.id}?if_seq_no=${startedRollup.seqNo}&if_primary_term=${startedRollup.primaryTerm}",
            emptyMap(), rollup.copy(enabled = true).toHttpEntity(),
        )
        // Second run, backing index is setup just like any other rollup index
        updateRollupStartTime(rollup)

        startedRollup =
            waitFor {
                val rollupJob = getRollup(rollupId = rollup.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata1 = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata1.status)
                rollupJob
            }

        rollupMetadataID = startedRollup.metadataID!!
        rollupMetadata = getRollupMetadata(rollupMetadataID)

        assertTrue("Did not process any doc during rollup", rollupMetadata.stats.documentsProcessed > 0)
    }

    /**
     * Index with alias is created and job1 successfully ran first time.
     * Then Job2 ran on first backing index once and made this alias invalid for further use by any rollup job
     */
    fun `test rollup action with alias as target_index with multiple backing indices failed`() {
        generateNYCTaxiData("source_runner_sixth_2123")

        // Create index with alias, without mappings
        val indexAlias = "alias_as_target_index_failed"
        val backingIndex1 = "backing_target_index1_f-000001"
        val backingIndex2 = "backing_target_index1_f-000002"
        val builtSettings =
            Settings.builder().let {
                it.put(INDEX_NUMBER_OF_REPLICAS, "1")
                it.put(INDEX_NUMBER_OF_SHARDS, "1")
                it
            }.build()
        val aliases = "\"$indexAlias\": { \"is_write_index\": true }"
        createIndex(backingIndex1, builtSettings, null, aliases)

        refreshAllIndices()

        val job1 =
            Rollup(
                id = "rollup_with1_alias_1",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic change of page size",
                sourceIndex = "source_runner_sixth_2123",
                targetIndex = indexAlias,
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 1000,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()),
                    ),
                ),
            ).let { createRollup(it, it.id) }

        // First run, backing index is empty: no mappings, no rollup_index setting, no rollupjobs in _META
        updateRollupStartTime(job1)

        waitFor { assertTrue("Target rollup index was not created", indexExists(backingIndex1)) }

        var startedRollup1 =
            waitFor {
                val rollupJob = getRollup(rollupId = job1.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
                assertTrue("Rollup is not disabled", !rollupJob.enabled)
                rollupJob
            }
        var rollupMetadataID = startedRollup1.metadataID!!
        var rollupMetadata = getRollupMetadata(rollupMetadataID)
        assertTrue("Did not process any doc during rollup", rollupMetadata.stats.documentsProcessed > 0)

        // Run job #2 on same target_index
        val job2 =
            job1.copy(id = "some_other_job_999", targetIndex = backingIndex1)
                .let { createRollup(it, it.id) }

        // Job2 First run, it should add itself to _meta in the same index job1 did.
        updateRollupStartTime(job2)

        var startedRollup2 =
            waitFor {
                val rollupJob = getRollup(rollupId = job2.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata1 = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata1.status)
                assertTrue("Rollup is not disabled", !rollupJob.enabled)
                rollupJob
            }
        rollupMetadataID = startedRollup2.metadataID!!
        rollupMetadata = getRollupMetadata(rollupMetadataID)
        assertTrue("Did not process any doc during rollup", rollupMetadata.stats.documentsProcessed > 0)

        // do rollover on alias
        val rolloverResponse = client().makeRequest("POST", "/$indexAlias/_rollover")
        assertEquals(RestStatus.OK, rolloverResponse.restStatus())
        waitFor { assertTrue("index was not created after rollover", indexExists(backingIndex2)) }

        refreshAllIndices()

        // restart job #1
        client().makeRequest(
            "PUT",
            "$ROLLUP_JOBS_BASE_URI/${startedRollup1.id}?if_seq_no=${startedRollup1.seqNo}&if_primary_term=${startedRollup1.primaryTerm}",
            emptyMap(), job1.copy(enabled = true).toHttpEntity(),
        )
        // Second run, backing index is setup just like any other rollup index
        updateRollupStartTime(job1)

        startedRollup1 =
            waitFor {
                val rollupJob = getRollup(rollupId = job1.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata1 = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FAILED, rollupMetadata1.status)
                rollupJob
            }

        rollupMetadataID = startedRollup1.metadataID!!
        rollupMetadata = getRollupMetadata(rollupMetadataID)

        assertEquals("Backing index [$backingIndex1] has multiple rollup job owners", rollupMetadata.failureReason)
    }

    fun `test rollup action with alias as target_index reuse failed`() {
        generateNYCTaxiData("source_runner_sixth_2209")

        // Create index with alias, without mappings
        val indexAlias = "alias_as_target_index_failed_1"
        val backingIndex1 = "backing-000001"
        val builtSettings =
            Settings.builder().let {
                it.put(INDEX_NUMBER_OF_REPLICAS, "1")
                it.put(INDEX_NUMBER_OF_SHARDS, "1")
                it
            }.build()
        val aliases = "\"$indexAlias\": { \"is_write_index\": true }"
        createIndex(backingIndex1, builtSettings, null, aliases)

        refreshAllIndices()

        val job1 =
            Rollup(
                id = "rollup_with_alias_11",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic change of page size",
                sourceIndex = "source_runner_sixth_2209",
                targetIndex = indexAlias,
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 1000,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()),
                    ),
                ),
            ).let { createRollup(it, it.id) }

        // First run, backing index is empty: no mappings, no rollup_index setting, no rollupjobs in _META
        updateRollupStartTime(job1)

        waitFor { assertTrue("Target rollup index was not created", indexExists(backingIndex1)) }

        val startedRollup1 =
            waitFor {
                val rollupJob = getRollup(rollupId = job1.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
                assertTrue("Rollup is not disabled", !rollupJob.enabled)
                rollupJob
            }
        var rollupMetadataID = startedRollup1.metadataID!!
        var rollupMetadata = getRollupMetadata(rollupMetadataID)
        assertTrue("Did not process any doc during rollup", rollupMetadata.stats.documentsProcessed > 0)

        // Run job #2 on same target_index alias
        val job2 =
            job1.copy(id = "some_other_job_9991", targetIndex = indexAlias)
                .let { createRollup(it, it.id) }

        // Job2 First run, it should fail because job1 already wrote to backing index
        updateRollupStartTime(job2)

        val startedRollup2 =
            waitFor {
                val rollupJob = getRollup(rollupId = job2.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata1 = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FAILED, rollupMetadata1.status)
                assertTrue("Rollup is not disabled", !rollupJob.enabled)
                rollupJob
            }
        rollupMetadataID = startedRollup2.metadataID!!
        rollupMetadata = getRollupMetadata(rollupMetadataID)
        assertEquals("More than one rollup jobs present on the backing index of the target alias, cannot perform rollup to this target alias [$indexAlias].", rollupMetadata.failureReason)
    }

    fun `test rollup action with alias as target_index multiple empty backing indices`() {
        generateNYCTaxiData("source_runner_sixth_1532209")

        // Create index with alias, without mappings
        val indexAlias = "alias_as_target_index_failed_19941"
        val backingIndex1 = "backing-99000001"
        val backingIndex2 = "backing-99000002"
        val builtSettings =
            Settings.builder().let {
                it.put(INDEX_NUMBER_OF_REPLICAS, "1")
                it.put(INDEX_NUMBER_OF_SHARDS, "1")
                it
            }.build()
        var aliases = "\"$indexAlias\": { \"is_write_index\": true }"
        createIndex(backingIndex1, builtSettings, null, aliases)
        aliases = "\"$indexAlias\": {}"
        createIndex(backingIndex2, builtSettings, null, aliases)

        refreshAllIndices()

        val job1 =
            Rollup(
                id = "rollup_with_alias_99243411",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic change of page size",
                sourceIndex = "source_runner_sixth_1532209",
                targetIndex = indexAlias,
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 1000,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()),
                    ),
                ),
            ).let { createRollup(it, it.id) }

        // First run, backing index is empty: no mappings, no rollup_index setting, no rollupjobs in _META
        updateRollupStartTime(job1)

        waitFor { assertTrue("Target rollup index was not created", indexExists(backingIndex1)) }

        var startedRollup1 =
            waitFor {
                val rollupJob = getRollup(rollupId = job1.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FAILED, rollupMetadata.status)
                assertTrue("Rollup is not disabled", !rollupJob.enabled)
                rollupJob
            }
        var rollupMetadataID = startedRollup1.metadataID!!
        var rollupMetadata = getRollupMetadata(rollupMetadataID)
        assertEquals("Backing index [$backingIndex2] has to have owner rollup job with id:[${startedRollup1.id}]", rollupMetadata.failureReason)
    }

    fun `test rollup with date_nanos as date_histogram field`() {
        val index = "date-nanos-index"
        val rollupIndex = "date-nanos-index-rollup"
        createIndex(
            index,
            Settings.EMPTY,
            """"properties": {
                  "purchaseDate": {
                    "type": "date_nanos" 
                  },
                  "itemName": {
                    "type": "keyword"
                  },
                  "itemPrice": {
                    "type": "float"
                  }
                }""",
        )

        indexDoc(index, "1", """{"purchaseDate": 1683149130000.6497, "itemName": "shoes", "itemPrice": 100.5}""".trimIndent())
        indexDoc(index, "2", """{"purchaseDate": 1683494790000, "itemName": "shoes", "itemPrice": 30.0}""".trimIndent())
        indexDoc(index, "3", """{"purchaseDate": "2023-05-08T18:57:33.743656789Z", "itemName": "shoes", "itemPrice": 60.592}""".trimIndent())

        refreshAllIndices()

        val job =
            Rollup(
                id = "rollup_with_alias_992434131",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.DAYS),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic change of page size",
                sourceIndex = index,
                targetIndex = rollupIndex,
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 1000,
                delay = 0,
                continuous = true,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "purchaseDate", fixedInterval = "5d"),
                    Terms("itemName", "itemName"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "itemPrice",
                        targetField = "itemPrice",
                        metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()),
                    ),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(job)

        waitFor { assertTrue("Target rollup index was not created", indexExists(rollupIndex)) }

        waitFor {
            val rollupJob = getRollup(rollupId = job.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not started", RollupMetadata.Status.STARTED, rollupMetadata.status)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test rollup with max metric when metric property not present`() {
        val sourceIdxTestName = "source_idx_test_max"
        val targetIdxTestName = "target_idx_test_max"
        val propertyName = "message.bytes_in"
        val maxMetricName = "min_message_bytes_in"

        generateMessageLogsData(sourceIdxTestName)
        val rollup =
            Rollup(
                id = "rollup_test_max",
                schemaVersion = 1L,
                enabled = true,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic stats test",
                sourceIndex = sourceIdxTestName,
                targetIndex = targetIdxTestName,
                targetIndexSettings = null,
                metadataID = null,
                roles = emptyList(),
                pageSize = 100,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "message.timestamp_received", targetField = "message.timestamp_received", fixedInterval = "10m"),
                    Terms("message.plugin", "message.plugin"),
                ),
                metrics =
                listOf(
                    RollupMetrics(sourceField = propertyName, targetField = propertyName, metrics = listOf(Max())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor { assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex)) }

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)

            // Term query
            val req =
                """
                {
                    "size": 0,
                    "query": {
                      "match_all": {}
                    },
                    "aggs": {
                        "$maxMetricName": {
                            "max": {
                                "field": "$propertyName"
                            }
                        }
                    }
                }
                """.trimIndent()
            var rawRes = client().makeRequest(RestRequest.Method.POST.name, "/$sourceIdxTestName/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            assertTrue(rawRes.restStatus() == RestStatus.OK)
            var rollupRes = client().makeRequest(RestRequest.Method.POST.name, "/$targetIdxTestName/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            assertTrue(rollupRes.restStatus() == RestStatus.OK)
            var rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
            var rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
            assertEquals(
                "Source and rollup index did not return same max results",
                rawAggRes.getValue(maxMetricName)["value"],
                rollupAggRes.getValue(maxMetricName)["value"],
            )
        }
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

    fun `test rollup on rollup with valid subset fields`() {
        val sourceIdxName = "source_rollup_validation"
        val firstRollupIdxName = "first_rollup_validation"
        val secondRollupIdxName = "second_rollup_validation"

        generateNYCTaxiData(sourceIdxName)

        // Create first level rollup
        val firstRollup = Rollup(
            id = "first_rollup_job",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "First level rollup",
            sourceIndex = sourceIdxName,
            targetIndex = firstRollupIdxName,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms(sourceField = "PULocationID", targetField = "PULocationID"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum())),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Average())),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(firstRollup)
        waitFor {
            val rollupJob = getRollup(rollupId = firstRollup.id)
            assertNotNull("First rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("First rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        // Create second level rollup with subset of fields (should succeed)
        val secondRollup = Rollup(
            id = "second_rollup_job",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Second level rollup with subset",
            sourceIndex = firstRollupIdxName,
            targetIndex = secondRollupIdxName,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1d"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum())),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(secondRollup)
        waitFor {
            val rollupJob = getRollup(rollupId = secondRollup.id)
            assertNotNull("Second rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Second rollup should succeed with subset fields", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }
    }

    fun `test rollup on rollup with invalid metric fields`() {
        val sourceIdxName = "source_rollup_invalid_metrics"
        val firstRollupIdxName = "first_rollup_invalid_metrics"
        val secondRollupIdxName = "second_rollup_invalid_metrics"

        generateNYCTaxiData(sourceIdxName)

        // Create first level rollup
        val firstRollup = Rollup(
            id = "first_rollup_invalid_metrics_job",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "First level rollup for invalid test",
            sourceIndex = sourceIdxName,
            targetIndex = firstRollupIdxName,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum())),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(firstRollup)
        waitFor {
            val rollupJob = getRollup(rollupId = firstRollup.id)
            assertNotNull("First rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("First rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        // Create second level rollup with invalid field (should fail)
        val secondRollup = Rollup(
            id = "second_rollup_invalid_metrics_job",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Second level rollup with invalid field",
            sourceIndex = firstRollupIdxName,
            targetIndex = secondRollupIdxName,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1d"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Average())), // This field doesn't exist in first rollup
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(secondRollup)
        waitFor {
            val rollupJob = getRollup(rollupId = secondRollup.id)
            assertNotNull("Second rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Second rollup should fail with invalid field", RollupMetadata.Status.FAILED, rollupMetadata.status)
            assertTrue(
                "Failure reason should mention invalid metric field",
                rollupMetadata.failureReason?.contains("Target rollup requests metrics") == true,
            )
        }
    }

    fun `test rollup on rollup with invalid timestamp interval`() {
        val sourceIdxName = "source_rollup_invalid_interval"
        val firstRollupIdxName = "first_rollup_invalid_interval"
        val secondRollupIdxName = "second_rollup_invalid_interval"

        generateNYCTaxiData(sourceIdxName)

        // Create first level rollup
        val firstRollup = Rollup(
            id = "first_rollup_invalid_interval_job",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "First level rollup for invalid test",
            sourceIndex = sourceIdxName,
            targetIndex = firstRollupIdxName,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "10m"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum())),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(firstRollup)
        waitFor {
            val rollupJob = getRollup(rollupId = firstRollup.id)
            assertNotNull("First rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("First rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        // Create second level rollup with invalid field (should fail)
        val secondRollup = Rollup(
            id = "second_rollup_invalid_interval_job",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Second level rollup with invalid field",
            sourceIndex = firstRollupIdxName,
            targetIndex = secondRollupIdxName,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "15m"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Average())), // This field doesn't exist in first rollup
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(secondRollup)
        waitFor {
            val rollupJob = getRollup(rollupId = secondRollup.id)
            assertNotNull("Second rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Second rollup should fail with invalid field", RollupMetadata.Status.FAILED, rollupMetadata.status)
            assertTrue(
                "Failure reason should mention invalid metric field",
                rollupMetadata.failureReason?.contains("must be an exact multiple of source interval") == true,
            )
        }
    }

    fun `test rollup on rollup with invalid fields`() {
        val sourceIdxName = "source_rollup_invalid_fields"
        val firstRollupIdxName = "first_rollup_invalid_fields"
        val secondRollupIdxName = "second_rollup_invalid_fields"

        generateNYCTaxiData(sourceIdxName)

        // Create first level rollup
        val firstRollup = Rollup(
            id = "first_rollup_invalid_fields_job",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "First level rollup for invalid test",
            sourceIndex = sourceIdxName,
            targetIndex = firstRollupIdxName,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1s"),
                Terms("RatecodeID", "RatecodeID"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum())),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(firstRollup)
        waitFor {
            val rollupJob = getRollup(rollupId = firstRollup.id)
            assertNotNull("First rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("First rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        // Create second level rollup with invalid field (should fail)
        val secondRollup = Rollup(
            id = "second_rollup_invalid_fields_job",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Second level rollup with invalid field",
            sourceIndex = firstRollupIdxName,
            targetIndex = secondRollupIdxName,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "5s"),
                Terms("passenger_count", "passenger_count"),
            ),
            metrics = listOf(
                RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum())),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(secondRollup)
        waitFor {
            val rollupJob = getRollup(rollupId = secondRollup.id)
            assertNotNull("Second rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Second rollup should fail with invalid field", RollupMetadata.Status.FAILED, rollupMetadata.status)
            assertTrue(
                "Failure reason should mention invalid metric field",
                rollupMetadata.failureReason?.contains("that don't exist in source rollup") == true,
            )
        }
    }

    fun `test multi tier rollup with all metrics 3 levels`() {
        val sourceIdx = "ecommerce_raw_data"
        val level1Idx = "ecommerce_hourly_rollup"
        val level2Idx = "ecommerce_daily_rollup"
        val level3Idx = "ecommerce_weekly_rollup"

        // Create source data with NYC taxi data
        generateNYCTaxiData(sourceIdx)

        // Level 1: Raw data -> Hourly rollup (all metrics)
        val level1Rollup = Rollup(
            id = "ecommerce_level1_rollup",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Level 1: Hourly rollup with all metrics",
            sourceIndex = sourceIdx,
            targetIndex = level1Idx,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms(sourceField = "PULocationID", targetField = "PULocationID"),
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(Sum(), Average(), Min(), Max(), ValueCount()),
                ),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(level1Rollup)
        waitFor {
            val rollupJob = getRollup(rollupId = level1Rollup.id)
            assertNotNull("Level 1 rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Level 1 rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        // Level 2: Hourly -> Daily rollup
        val level2Rollup = Rollup(
            id = "ecommerce_level2_rollup",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Level 2: Daily rollup from hourly data",
            sourceIndex = level1Idx,
            targetIndex = level2Idx,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1d"),
                Terms(sourceField = "PULocationID", targetField = "PULocationID"),
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(Sum(), Average(), Min(), Max(), ValueCount()),
                ),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(level2Rollup)
        waitFor {
            val rollupJob = getRollup(rollupId = level2Rollup.id)
            assertNotNull("Level 2 rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Level 2 rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        // Level 3: Daily -> Weekly rollup
        val level3Rollup = Rollup(
            id = "ecommerce_level3_rollup",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Level 3: Weekly rollup from daily data",
            sourceIndex = level2Idx,
            targetIndex = level3Idx,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "7d"),
                Terms(sourceField = "PULocationID", targetField = "PULocationID"),
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(Sum(), Average(), Min(), Max(), ValueCount()),
                ),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(level3Rollup)
        waitFor {
            val rollupJob = getRollup(rollupId = level3Rollup.id)
            assertNotNull("Level 3 rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Level 3 rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)

            // Verify data consistency across all levels
            verifyMultiTierRollupData(sourceIdx, level3Idx)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test continuous rollup on rollup index`() {
        val sourceIdx = "continuous_source_data"
        val level1Idx = "continuous_level1_rollup"
        val level2Idx = "continuous_level2_rollup"

        generateNYCTaxiData(sourceIdx)

        // Level 1: Non-continuous rollup to create rollup index
        val level1Rollup = Rollup(
            id = "continuous_level1_job",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Level 1: Create rollup index",
            sourceIndex = sourceIdx,
            targetIndex = level1Idx,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1d"),
                Terms(sourceField = "PULocationID", targetField = "PULocationID"),
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(Sum(), Average(), Min(), Max(), ValueCount()),
                ),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(level1Rollup)
        waitFor {
            val rollupJob = getRollup(rollupId = level1Rollup.id)
            assertNotNull("Level 1 rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Level 1 rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        // Level 2: Continuous rollup on rollup index
        val level2Rollup = Rollup(
            id = "continuous_level2_job",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "Level 2: Continuous rollup on rollup index",
            sourceIndex = level1Idx,
            targetIndex = level2Idx,
            targetIndexSettings = null,
            metadataID = null,
            roles = emptyList(),
            pageSize = 100,
            delay = 100,
            continuous = true,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "7d"),
                Terms(sourceField = "PULocationID", targetField = "PULocationID"),
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(Sum(), Average(), Min(), Max(), ValueCount()),
                ),
            ),
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(level2Rollup)
        waitFor {
            val rollupJob = getRollup(rollupId = level2Rollup.id)
            assertNotNull("Level 2 rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertNotNull("Level 2 continuous metadata not found", rollupMetadata.continuous)
            assertEquals("Level 2 rollup is not started", RollupMetadata.Status.STARTED, rollupMetadata.status)
            assertTrue("Level 2 rollup processed no documents", rollupMetadata.stats.documentsProcessed > 0)
            assertTrue("Level 2 rollup indexed no rollups", rollupMetadata.stats.rollupsIndexed > 0)

            // Verify metrics are consistent between source and level 2 rollup
            val aggReq = """
                {
                    "size": 0,
                    "query": { "match_all": {} },
                    "aggs": {
                        "sum_passenger": { "sum": { "field": "passenger_count" } },
                        "min_passenger": { "min": { "field": "passenger_count" } },
                        "max_passenger": { "max": { "field": "passenger_count" } },
                        "value_count_passenger": { "value_count": { "field": "passenger_count" } },
                        "avg_passenger": { "avg": { "field": "passenger_count" } }
                    }
                }
            """.trimIndent()

            val sourceRes = client().makeRequest(
                RestRequest.Method.POST.name,
                "/$sourceIdx/_search",
                emptyMap(),
                StringEntity(aggReq, ContentType.APPLICATION_JSON),
            )
            val level2Res = client().makeRequest(
                RestRequest.Method.POST.name,
                "/$level2Idx/_search",
                emptyMap(),
                StringEntity(aggReq, ContentType.APPLICATION_JSON),
            )

            val sourceAggs = sourceRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
            val level2Aggs = level2Res.asMap()["aggregations"] as Map<String, Map<String, Any>>

            assertEquals(
                "Sum not consistent",
                sourceAggs["sum_passenger"]!!["value"],
                level2Aggs["sum_passenger"]!!["value"],
            )
            assertEquals(
                "Min not consistent",
                sourceAggs["min_passenger"]!!["value"],
                level2Aggs["min_passenger"]!!["value"],
            )
            assertEquals(
                "Max not consistent",
                sourceAggs["max_passenger"]!!["value"],
                level2Aggs["max_passenger"]!!["value"],
            )
            assertEquals(
                "Value count not consistent",
                sourceAggs["value_count_passenger"]!!["value"],
                level2Aggs["value_count_passenger"]!!["value"],
            )
            assertEquals(
                "Average not consistent",
                sourceAggs["avg_passenger"]!!["value"],
                level2Aggs["avg_passenger"]!!["value"],
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun verifyMultiTierRollupData(sourceIdx: String, level3Idx: String) {
        // Verify all 5 metrics are consistent between source and final rollup
        val aggReq = """
            {
                "size": 0,
                "query": {
                  "match_all": {}
                },
                "aggs": {
                    "sum_passenger_count": {
                        "sum": { "field": "passenger_count" }
                    },
                    "avg_passenger_count": {
                        "avg": { "field": "passenger_count" }
                    },
                    "min_passenger_count": {
                        "min": { "field": "passenger_count" }
                    },
                    "max_passenger_count": {
                        "max": { "field": "passenger_count" }
                    },
                    "count_passenger_count": {
                        "value_count": { "field": "passenger_count" }
                    }
                }
            }
        """.trimIndent()

        val sourceResponse = client().makeRequest(RestRequest.Method.POST.name, "/$sourceIdx/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
        assertTrue("Source search should succeed", sourceResponse.restStatus() == RestStatus.OK)

        val level3Response = client().makeRequest(RestRequest.Method.POST.name, "/$level3Idx/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
        assertTrue("Level 3 search should succeed", level3Response.restStatus() == RestStatus.OK)

        val sourceAggs = sourceResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val level3Aggs = level3Response.asMap()["aggregations"] as Map<String, Map<String, Any>>

        // Verify all 5 metrics
        assertEquals(
            "Sum should be consistent",
            sourceAggs["sum_passenger_count"]!!["value"],
            level3Aggs["sum_passenger_count"]!!["value"],
        )
        assertEquals(
            "Average should be consistent",
            sourceAggs["avg_passenger_count"]!!["value"],
            level3Aggs["avg_passenger_count"]!!["value"],
        )
        assertEquals(
            "Min should be consistent",
            sourceAggs["min_passenger_count"]!!["value"],
            level3Aggs["min_passenger_count"]!!["value"],
        )
        assertEquals(
            "Max should be consistent",
            sourceAggs["max_passenger_count"]!!["value"],
            level3Aggs["max_passenger_count"]!!["value"],
        )

        assertEquals(
            "Value count should be consistent",
            sourceAggs["count_passenger_count"]!!["value"],
            level3Aggs["count_passenger_count"]!!["value"],
        )
    }

    private fun deleteRollupMetadata(metadataId: String) {
        val response = adminClient().makeRequest("DELETE", "${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}/_doc/$metadataId")
        assertEquals("Unable to delete rollup metadata $metadataId", RestStatus.OK, response.restStatus())
    }
}
