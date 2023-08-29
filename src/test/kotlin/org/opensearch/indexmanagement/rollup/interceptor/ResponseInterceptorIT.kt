/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
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
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit

@Suppress("UNCHECKED_CAST")
class ResponseInterceptorIT : RollupRestTestCase() {
    fun `test search a live index and rollup index with no overlap`() {
        generateNYCTaxiData("source_rollup_search")
        val rollup = Rollup(
            id = "basic_term_query_rollup_search",
            enabled = true,
            schemaVersion = 1L,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_rollup_search",
            targetIndex = "target_rollup_search",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("passenger_count", "passenger_count")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                )
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()
        // Split data at 1546304400000 or Jan 01 2019 01:00:00
        // Delete half the values from live data simulating an ism job deleting old data
        var r = """
            {
              "query": {
                "range": {
                  "tpep_pickup_datetime": {
                    "lt": 1546304400000,
                    "format": "epoch_millis",
                    "time_zone": "+00:00"
                  }
                }
              }
            }
        """.trimIndent()
        var deleteLiveResponse = client().makeRequest(
            "POST",
            "source_rollup_search/_delete_by_query",
            mapOf("refresh" to "true"),
            StringEntity(r, ContentType.APPLICATION_JSON)
        )

        assertTrue("Could not delete live data", deleteLiveResponse.restStatus() == RestStatus.OK)

        // Delete half the values from rollup data
        r = """
            {
              "query": {
                "range": {
                  "tpep_pickup_datetime": {
                    "gte": 1546304400000,
                    "format": "epoch_millis",
                    "time_zone": "+00:00"
                  }
                }
              }
            }
        """.trimIndent()
        var deleteRollupResponse = client().makeRequest(
            "POST",
            "target_rollup_search/_delete_by_query",
            mapOf("refresh" to "true"),
            StringEntity(r, ContentType.APPLICATION_JSON)
        )

        assertTrue("Could not delete rollup data", deleteRollupResponse.restStatus() == RestStatus.OK)
        // Search both and check if time series data is the same
        var req = """
            {
                "size": 0,
                "query": {
                    "match_all": {}
                },
                "aggs": {
                    "sum_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        var searchResponse = client().makeRequest("POST", "/target_rollup_search,source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(searchResponse.restStatus() == RestStatus.OK)
        var responseAggs = searchResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Aggregation from searching both indices is wrong",
            9024.0,
            responseAggs.getValue("sum_passenger_count")["value"]
        )
    }
    // Edge Case
    fun `test search a live index with no data and rollup index with data`() {
        generateNYCTaxiData("source_rollup_search")
        val rollup = Rollup(
            id = "basic_term_query_rollup_search",
            enabled = true,
            schemaVersion = 1L,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_rollup_search",
            targetIndex = "target_rollup_search",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("passenger_count", "passenger_count")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                )
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        // Delete values from live index
        var deleteResponse = client().makeRequest(
            "POST",
            "source_rollup_search/_delete_by_query",
            mapOf("refresh" to "true"),
            StringEntity("""{"query": {"match_all": {}}}""", ContentType.APPLICATION_JSON)
        )
        assertTrue(deleteResponse.restStatus() == RestStatus.OK)
        // Term query
        var req = """
            {
                "size": 0,
                "query": {
                    "match_all": {}
                },
                "aggs": {
                    "sum_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        var searchResponse = client().makeRequest("POST", "/target_rollup_search,source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(searchResponse.restStatus() == RestStatus.OK)
        var responseAggs = searchResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Aggregation from searching both indices is wrong",
            9024.0,
            responseAggs.getValue("sum_passenger_count")["value"]
        )
    }
    fun `test search a live index and rollup index with data overlap`() {
        generateNYCTaxiData("source_rollup_search")
        val rollup = Rollup(
            id = "basic_term_query_rollup_search",
            enabled = true,
            schemaVersion = 1L,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_rollup_search",
            targetIndex = "target_rollup_search",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("passenger_count", "passenger_count")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                )
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()
        // Split data at 1546304400000 or Jan 01 2019 01:00:00
        // Delete half the values from live data simulating an ism job deleting old data
        var r = """
            {
              "query": {
                "range": {
                  "tpep_pickup_datetime": {
                    "lt": 1546304400000,
                    "format": "epoch_millis",
                    "time_zone": "+00:00"
                  }
                }
              }
            }
        """.trimIndent()
        var deleteLiveResponse = client().makeRequest(
            "POST",
            "source_rollup_search/_delete_by_query",
            mapOf("refresh" to "true"),
            StringEntity(r, ContentType.APPLICATION_JSON)
        )

        assertTrue("Could not delete live data", deleteLiveResponse.restStatus() == RestStatus.OK)
        // Rollup index is complete overlap of live data
        // Search both and check if time series data is the same
        var req = """
            {
                "size": 0,
                "query": {
                    "match_all": {}
                },
                "aggs": {
                    "sum_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        var searchResponse = client().makeRequest("POST", "/target_rollup_search,source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(searchResponse.restStatus() == RestStatus.OK)
        var responseAggs = searchResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Aggregation from searching both indices is wrong",
            9024.0,
            responseAggs.getValue("sum_passenger_count")["value"]
        )
    }
    fun `test min, max, value_count, sum, and avg aggs on data`() {
        // TODO add avg computation later
        generateNYCTaxiData("source_rollup_search")
        val rollup = Rollup(
            id = "basic_term_query_rollup_search",
            enabled = true,
            schemaVersion = 1L,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_rollup_search",
            targetIndex = "target_rollup_search",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("passenger_count", "passenger_count")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                )
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        // Delete values from live index
        var deleteResponse = client().makeRequest(
            "POST",
            "source_rollup_search/_delete_by_query",
            mapOf("refresh" to "true"),
            StringEntity("""{"query": {"match_all": {}}}""", ContentType.APPLICATION_JSON)
        )
        assertTrue(deleteResponse.restStatus() == RestStatus.OK)
        var req = """
            {
                "size": 0,
                "query": {
                    "match_all": {}
                },
                "aggs": {
                    "sum_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    },
                    "max_passenger_count": {
                        "max": {
                            "field": "passenger_count"
                        }
                    },
                    "min_passenger_count": {
                        "min": {
                            "field": "passenger_count"
                        }
                    },
                    "avg_passenger_count": {
                        "avg": {
                            "field": "passenger_count"
                        }
                    },
                    "count_passenger_count": {
                        "value_count": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
        """.trimIndent()
        var searchResponse = client().makeRequest("POST", "/target_rollup_search,source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(searchResponse.restStatus() == RestStatus.OK)
        var responseAggs = searchResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "sum agg is wrong",
            9024.0,
            responseAggs.getValue("sum_passenger_count")["value"]
        )
        assertEquals(
            "max agg is wrong",
            6.0,
            responseAggs.getValue("max_passenger_count")["value"]
        )
        assertEquals(
            "min agg is wrong",
            0.0,
            responseAggs.getValue("min_passenger_count")["value"]
        )
        assertEquals(
            "value_count is wrong",
            5000,
            responseAggs.getValue("count_passenger_count")["value"]
        )
        assertEquals(
            "avg is wrong",
            1.8048,
            responseAggs.getValue("avg_passenger_count")["value"]
        )
    }
}
