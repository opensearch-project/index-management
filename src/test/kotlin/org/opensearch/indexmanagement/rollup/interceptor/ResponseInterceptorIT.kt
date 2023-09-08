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
            id = "base_case1_rollup_search",
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
        // Get expected aggregation values by searching live data before deletion
        var aggReq = """
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
        var searchResponse = client().makeRequest("POST", "/source_rollup_search/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
        assertTrue("Could not search inital data for expected values", searchResponse.restStatus() == RestStatus.OK)
        var expectedAggs = searchResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val expectedSum = expectedAggs.getValue("sum_passenger_count")["value"]
        val expectedMax = expectedAggs.getValue("max_passenger_count")["value"]
        val expectedMin = expectedAggs.getValue("min_passenger_count")["value"]
        val expectedCount = expectedAggs.getValue("count_passenger_count")["value"]
        val expectedAvg = expectedAggs.getValue("avg_passenger_count")["value"]
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
        var searchBothResponse = client().makeRequest("POST", "/target_rollup_search,source_rollup_search/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
        assertTrue(searchBothResponse.restStatus() == RestStatus.OK)
        var responseAggs = searchBothResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "sum agg is wrong",
            expectedSum,
            responseAggs.getValue("sum_passenger_count")["value"]
        )
        assertEquals(
            "max agg is wrong",
            expectedMax,
            responseAggs.getValue("max_passenger_count")["value"]
        )
        assertEquals(
            "min agg is wrong",
            expectedMin,
            responseAggs.getValue("min_passenger_count")["value"]
        )
        assertEquals(
            "value_count is wrong",
            expectedCount,
            responseAggs.getValue("count_passenger_count")["value"]
        )
        assertEquals(
            "avg is wrong",
            expectedAvg,
            responseAggs.getValue("avg_passenger_count")["value"]
        )
    }
//    // Edge Case
//    fun `test search a live index with no data and rollup index with data`() {
//        generateNYCTaxiData("source_rollup_search_no_data_case")
//        val rollup = Rollup(
//            id = "base_case_2_rollup_search",
//            enabled = true,
//            schemaVersion = 1L,
//            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
//            jobLastUpdatedTime = Instant.now(),
//            jobEnabledTime = Instant.now(),
//            description = "basic search test",
//            sourceIndex = "source_rollup_search_no_data_case",
//            targetIndex = "target_rollup_search_no_data_case",
//            metadataID = null,
//            roles = emptyList(),
//            pageSize = 10,
//            delay = 0,
//            continuous = false,
//            dimensions = listOf(
//                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
//                Terms("passenger_count", "passenger_count")
//            ),
//            metrics = listOf(
//                RollupMetrics(
//                    sourceField = "passenger_count", targetField = "passenger_count",
//                    metrics = listOf(
//                        Sum(), Min(), Max(),
//                        ValueCount(), Average()
//                    )
//                )
//            )
//        ).let { createRollup(it, it.id) }
//
//        updateRollupStartTime(rollup)
//
//        waitFor {
//            val rollupJob = getRollup(rollupId = rollup.id)
//            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
//            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
//            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
//        }
//
//        refreshAllIndices()
//        // Get expected aggregation values by searching live data before deletion
//        var aggReq = """
//            {
//                "size": 0,
//                "query": {
//                    "match_all": {}
//                },
//                "aggs": {
//                    "sum_passenger_count": {
//                        "sum": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "max_passenger_count": {
//                        "max": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "min_passenger_count": {
//                        "min": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "avg_passenger_count": {
//                        "avg": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "count_passenger_count": {
//                        "value_count": {
//                            "field": "passenger_count"
//                        }
//                    }
//                }
//            }
//        """.trimIndent()
//        var searchResponse = client().makeRequest("POST", "/source_rollup_search_no_data_case/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
//        assertTrue("Could not search inital data for expected values", searchResponse.restStatus() == RestStatus.OK)
//        var expectedAggs = searchResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
//        val expectedSum = expectedAggs.getValue("sum_passenger_count")["value"]
//        val expectedMax = expectedAggs.getValue("max_passenger_count")["value"]
//        val expectedMin = expectedAggs.getValue("min_passenger_count")["value"]
//        val expectedCount = expectedAggs.getValue("count_passenger_count")["value"]
//        val expectedAvg = expectedAggs.getValue("avg_passenger_count")["value"]
//        refreshAllIndices()
//        // Delete values from live index
//        var deleteResponse = client().makeRequest(
//            "POST",
//            "source_rollup_search_no_data_case/_delete_by_query",
//            mapOf("refresh" to "true"),
//            StringEntity("""{"query": {"match_all": {}}}""", ContentType.APPLICATION_JSON)
//        )
//        assertTrue(deleteResponse.restStatus() == RestStatus.OK)
//        var searchBothResponse = client().makeRequest("POST", "/target_rollup_search_no_data_case,source_rollup_search_no_data_case/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
//        assertTrue(searchBothResponse.restStatus() == RestStatus.OK)
//        var responseAggs = searchBothResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
//        assertEquals(
//            "sum agg is wrong",
//            expectedSum,
//            responseAggs.getValue("sum_passenger_count")["value"]
//        )
//        assertEquals(
//            "max agg is wrong",
//            expectedMax,
//            responseAggs.getValue("max_passenger_count")["value"]
//        )
//        assertEquals(
//            "min agg is wrong",
//            expectedMin,
//            responseAggs.getValue("min_passenger_count")["value"]
//        )
//        assertEquals(
//            "value_count is wrong",
//            expectedCount,
//            responseAggs.getValue("count_passenger_count")["value"]
//        )
//        assertEquals(
//            "avg is wrong",
//            expectedAvg,
//            responseAggs.getValue("avg_passenger_count")["value"]
//        )
//    }
//    fun `test search a live index and rollup index with data overlap`() {
//        generateNYCTaxiData("source_rollup_search_data_overlap_case")
//        val rollup = Rollup(
//            id = "case2_rollup_search",
//            enabled = true,
//            schemaVersion = 1L,
//            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
//            jobLastUpdatedTime = Instant.now(),
//            jobEnabledTime = Instant.now(),
//            description = "basic search test",
//            sourceIndex = "source_rollup_search_data_overlap_case",
//            targetIndex = "target_rollup_search_data_overlap_case",
//            metadataID = null,
//            roles = emptyList(),
//            pageSize = 10,
//            delay = 0,
//            continuous = false,
//            dimensions = listOf(
//                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
//                Terms("passenger_count", "passenger_count")
//            ),
//            metrics = listOf(
//                RollupMetrics(
//                    sourceField = "passenger_count", targetField = "passenger_count",
//                    metrics = listOf(
//                        Sum(), Min(), Max(),
//                        ValueCount(), Average()
//                    )
//                )
//            )
//        ).let { createRollup(it, it.id) }
//
//        updateRollupStartTime(rollup)
//
//        waitFor {
//            val rollupJob = getRollup(rollupId = rollup.id)
//            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
//            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
//            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
//        }
//        // Get expected aggregation values by searching live data before deletion
//        var aggReq = """
//            {
//                "size": 0,
//                "query": {
//                    "match_all": {}
//                },
//                "aggs": {
//                    "sum_passenger_count": {
//                        "sum": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "max_passenger_count": {
//                        "max": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "min_passenger_count": {
//                        "min": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "avg_passenger_count": {
//                        "avg": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "count_passenger_count": {
//                        "value_count": {
//                            "field": "passenger_count"
//                        }
//                    }
//                }
//            }
//        """.trimIndent()
//        var expectedSearchResponse = client().makeRequest("POST", "/source_rollup_search_data_overlap_case/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
//        assertTrue("Could not search inital data for expected values", expectedSearchResponse.restStatus() == RestStatus.OK)
//        var expectedAggs = expectedSearchResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
//        val expectedSum = expectedAggs.getValue("sum_passenger_count")["value"]
//        val expectedMax = expectedAggs.getValue("max_passenger_count")["value"]
//        val expectedMin = expectedAggs.getValue("min_passenger_count")["value"]
//        val expectedCount = expectedAggs.getValue("count_passenger_count")["value"]
//        val expectedAvg = expectedAggs.getValue("avg_passenger_count")["value"]
//
//        refreshAllIndices()
//        // Split data at 1546304400000 or Jan 01 2019 01:00:00
//        // Delete half the values from live data simulating an ism job deleting old data
//        var r = """
//            {
//              "query": {
//                "range": {
//                  "tpep_pickup_datetime": {
//                    "lt": 1546304400000,
//                    "format": "epoch_millis",
//                    "time_zone": "+00:00"
//                  }
//                }
//              }
//            }
//        """.trimIndent()
//        var deleteLiveResponse = client().makeRequest(
//            "POST",
//            "source_rollup_search_data_overlap_case/_delete_by_query",
//            mapOf("refresh" to "true"),
//            StringEntity(r, ContentType.APPLICATION_JSON)
//        )
//
//        assertTrue("Could not delete live data", deleteLiveResponse.restStatus() == RestStatus.OK)
//        // Rollup index is complete overlap of live data
//        // Search both and check if time series data is the same
//        var searchBothResponse = client().makeRequest("POST", "/target_rollup_search_data_overlap_case,source_rollup_search_data_overlap_case/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
//        assertTrue(searchBothResponse.restStatus() == RestStatus.OK)
//        var responseAggs = searchBothResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
//        assertEquals(
//            "sum agg is wrong",
//            expectedSum,
//            responseAggs.getValue("sum_passenger_count")["value"]
//        )
//        assertEquals(
//            "max agg is wrong",
//            expectedMax,
//            responseAggs.getValue("max_passenger_count")["value"]
//        )
//        assertEquals(
//            "min agg is wrong",
//            expectedMin,
//            responseAggs.getValue("min_passenger_count")["value"]
//        )
//        assertEquals(
//            "value_count is wrong",
//            expectedCount,
//            responseAggs.getValue("count_passenger_count")["value"]
//        )
//        assertEquals(
//            "avg is wrong",
//            expectedAvg,
//            responseAggs.getValue("avg_passenger_count")["value"]
//        )
//    }
//    fun `test search multiple live data indices and a rollup data index with overlap`() {
//        generateNYCTaxiData("source_rollup_search_multi_index_case")
//        val rollup = Rollup(
//            id = "case3_rollup_search",
//            enabled = true,
//            schemaVersion = 1L,
//            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
//            jobLastUpdatedTime = Instant.now(),
//            jobEnabledTime = Instant.now(),
//            description = "basic search test",
//            sourceIndex = "source_rollup_search_multi_index_case",
//            targetIndex = "target_rollup_search_multi_index_case",
//            metadataID = null,
//            roles = emptyList(),
//            pageSize = 10,
//            delay = 0,
//            continuous = false,
//            dimensions = listOf(
//                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
//                Terms("passenger_count", "passenger_count")
//            ),
//            metrics = listOf(
//                RollupMetrics(
//                    sourceField = "passenger_count", targetField = "passenger_count",
//                    metrics = listOf(
//                        Sum(), Min(), Max(),
//                        ValueCount(), Average()
//                    )
//                )
//            )
//        ).let { createRollup(it, it.id) }
//
//        updateRollupStartTime(rollup)
//
//        waitFor {
//            val rollupJob = getRollup(rollupId = rollup.id)
//            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
//            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
//            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
//        }
//
//        refreshAllIndices()
//        // Split data at 1546304400000 or Jan 01 2019 01:00:00
//        // Delete half the values from live data simulating an ism job deleting old data
//        var r = """
//            {
//              "query": {
//                "range": {
//                  "tpep_pickup_datetime": {
//                    "lt": 1546304400000,
//                    "format": "epoch_millis",
//                    "time_zone": "+00:00"
//                  }
//                }
//              }
//            }
//        """.trimIndent()
//        var deleteLiveResponse = client().makeRequest(
//            "POST",
//            "source_rollup_search_multi_index_case/_delete_by_query",
//            mapOf("refresh" to "true"),
//            StringEntity(r, ContentType.APPLICATION_JSON)
//        )
//
//        assertTrue("Could not delete live data", deleteLiveResponse.restStatus() == RestStatus.OK)
//
//        // Insert more live data
//        generateNYCTaxiData("source_rollup_search_multi_index_case2")
//        // Expected values would discard the overlapping rollup index completely
//        var aggReq = """
//            {
//                "size": 0,
//                "query": {
//                    "match_all": {}
//                },
//                "aggs": {
//                    "sum_passenger_count": {
//                        "sum": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "max_passenger_count": {
//                        "max": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "min_passenger_count": {
//                        "min": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "avg_passenger_count": {
//                        "avg": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "count_passenger_count": {
//                        "value_count": {
//                            "field": "passenger_count"
//                        }
//                    }
//                }
//            }
//        """.trimIndent()
//        var searchResponse = client().makeRequest("POST", "/source_rollup_search_multi_index_case,source_rollup_search_multi_index_case2/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
//        assertTrue("Could not search initial data for expected values", searchResponse.restStatus() == RestStatus.OK)
//        var expectedAggs = searchResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
//        val expectedSum = expectedAggs.getValue("sum_passenger_count")["value"]
//        val expectedMax = expectedAggs.getValue("max_passenger_count")["value"]
//        val expectedMin = expectedAggs.getValue("min_passenger_count")["value"]
//        val expectedCount = expectedAggs.getValue("count_passenger_count")["value"]
//        val expectedAvg = expectedAggs.getValue("avg_passenger_count")["value"]
//        refreshAllIndices()
//
//        // Search all 3 indices to check if overlap was removed
//        var searchAllResponse = client().makeRequest("POST", "/target_rollup_search_multi_index_case,source_rollup_search_multi_index_case,source_rollup_search_multi_index_case2/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
//        assertTrue(searchAllResponse.restStatus() == RestStatus.OK)
//        var responseAggs = searchAllResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
//        assertEquals(
//            "sum agg is wrong",
//            expectedSum,
//            responseAggs.getValue("sum_passenger_count")["value"]
//        )
//        assertEquals(
//            "max agg is wrong",
//            expectedMax,
//            responseAggs.getValue("max_passenger_count")["value"]
//        )
//        assertEquals(
//            "min agg is wrong",
//            expectedMin,
//            responseAggs.getValue("min_passenger_count")["value"]
//        )
//        assertEquals(
//            "value_count is wrong",
//            expectedCount,
//            responseAggs.getValue("count_passenger_count")["value"]
//        )
//        assertEquals(
//            "avg is wrong",
//            expectedAvg,
//            responseAggs.getValue("avg_passenger_count")["value"]
//        )
//    }
//    fun `test search aliased live indices data and rollup data`() {
//        /* add later */
//        // Create 3 indices with 5,000 docs each nyc-taxi-data-1, nyc-taxi-data-2, nyc-taxi-data-3
//        generateNYCTaxiData("nyc-taxi-data-1")
//        generateNYCTaxiData("nyc-taxi-data-2")
//        generateNYCTaxiData("nyc-taxi-data-3")
//        // Add them to alias nyc-taxi-data
//        val createAliasReq = """
//            {
//              "actions": [
//                {
//                  "add": {
//                    "index": "nyc-taxi-data-1",
//                    "alias": "nyc-taxi-data"
//                  }
//                },
//                {
//                  "add": {
//                    "index": "nyc-taxi-data-2",
//                    "alias": "nyc-taxi-data"
//                  }
//                },
//                {
//                  "add": {
//                    "index": "nyc-taxi-data-3",
//                    "alias": "nyc-taxi-data"
//                  }
//                }
//              ]
//            }
//        """.trimIndent()
//        val createAliasRes = client().makeRequest(
//            "POST",
//            "_aliases",
//            mapOf(),
//            StringEntity(createAliasReq, ContentType.APPLICATION_JSON)
//        )
//        assertTrue("Could not create alias", createAliasRes.restStatus() == RestStatus.OK)
//        // Rollup alias into rollup-nyc-taxi-data
//        val rollup = Rollup(
//            id = "alias_rollup_search",
//            enabled = true,
//            schemaVersion = 1L,
//            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
//            jobLastUpdatedTime = Instant.now(),
//            jobEnabledTime = Instant.now(),
//            description = "basic search test",
//            sourceIndex = "nyc-taxi-data",
//            targetIndex = "rollup-nyc-taxi-data",
//            metadataID = null,
//            roles = emptyList(),
//            pageSize = 10,
//            delay = 0,
//            continuous = false,
//            dimensions = listOf(
//                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
//                Terms("passenger_count", "passenger_count")
//            ),
//            metrics = listOf(
//                RollupMetrics(
//                    sourceField = "passenger_count", targetField = "passenger_count",
//                    metrics = listOf(
//                        Sum(), Min(), Max(),
//                        ValueCount(), Average()
//                    )
//                )
//            )
//        ).let { createRollup(it, it.id) }
//
//        updateRollupStartTime(rollup)
//
//        waitFor {
//            val rollupJob = getRollup(rollupId = rollup.id)
//            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
//            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
//            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
//        }
//        refreshAllIndices()
//        // Find expected values by searching nyc-taxi-data
//        var aggReq = """
//            {
//                "size": 0,
//                "query": {
//                    "match_all": {}
//                },
//                "aggs": {
//                    "sum_passenger_count": {
//                        "sum": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "max_passenger_count": {
//                        "max": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "min_passenger_count": {
//                        "min": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "avg_passenger_count": {
//                        "avg": {
//                            "field": "passenger_count"
//                        }
//                    },
//                    "count_passenger_count": {
//                        "value_count": {
//                            "field": "passenger_count"
//                        }
//                    }
//                }
//            }
//        """.trimIndent()
//        var searchResponse = client().makeRequest("POST", "/nyc-taxi-data/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
//        assertTrue("Could not search initial data for expected values", searchResponse.restStatus() == RestStatus.OK)
//        var expectedAggs = searchResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
//        val expectedSum = expectedAggs.getValue("sum_passenger_count")["value"]
//        val expectedMax = expectedAggs.getValue("max_passenger_count")["value"]
//        val expectedMin = expectedAggs.getValue("min_passenger_count")["value"]
//        val expectedCount = expectedAggs.getValue("count_passenger_count")["value"]
//        val expectedAvg = expectedAggs.getValue("avg_passenger_count")["value"]
//        refreshAllIndices()
//        // Validate result from searching rollup-nyc-taxi-data, searching nyc-taxi-data
//        val start = System.currentTimeMillis()
//        var searchAllResponse = client().makeRequest("POST", "/rollup-nyc-taxi-data,nyc-taxi-data/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
//        assertTrue(searchAllResponse.restStatus() == RestStatus.OK)
//        var responseAggs = searchAllResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
//        assertEquals(
//            "sum agg is wrong",
//            expectedSum,
//            responseAggs.getValue("sum_passenger_count")["value"]
//        )
//        assertEquals(
//            "max agg is wrong",
//            expectedMax,
//            responseAggs.getValue("max_passenger_count")["value"]
//        )
//        assertEquals(
//            "min agg is wrong",
//            expectedMin,
//            responseAggs.getValue("min_passenger_count")["value"]
//        )
//        assertEquals(
//            "value_count is wrong",
//            expectedCount,
//            responseAggs.getValue("count_passenger_count")["value"]
//        )
//        assertEquals(
//            "avg is wrong",
//            expectedAvg,
//            responseAggs.getValue("avg_passenger_count")["value"]
//        )
//        val elapsedTimeMs = System.currentTimeMillis() - start
//        assertEquals("ronsax search reqeust took $elapsedTimeMs ms", true, false)
//    }
}
