/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.junit.Assert
import org.opensearch.client.ResponseException
import org.opensearch.core.rest.RestStatus
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
import java.time.Instant
import java.time.temporal.ChronoUnit

@Suppress("UNCHECKED_CAST")
class RollupInterceptorIT : RollupRestTestCase() {
    fun `test roll up search`() {
        generateNYCTaxiData("source_rollup_search")
        val rollup =
            Rollup(
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
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        // Term query
        var req =
            """
            {
                "size": 0,
                "query": {
                    "term": {
                        "RatecodeID": 1
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        var rawRes = client().makeRequest("POST", "/source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        var rollupRes = client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        var rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        var rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("min_passenger_count")["value"],
            rollupAggRes.getValue("min_passenger_count")["value"],
        )

        // Terms query
        req =
            """
            {
                "size": 0,
                "query": {
                    "terms": {
                        "RatecodeID": [1, 2]
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("min_passenger_count")["value"],
            rollupAggRes.getValue("min_passenger_count")["value"],
        )

        // Range query
        req =
            """
            {
                "size": 0,
                "query": {
                    "range": {
                        "RatecodeID": {"gte": 1, "lt":2}
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("min_passenger_count")["value"],
            rollupAggRes.getValue("min_passenger_count")["value"],
        )

        // Bool query
        req =
            """
            {
                "size": 0,
                "query": {
                    "bool": {
                        "must_not": {"term": {"RatecodeID": 1}},
                        "must": {"range": {"RatecodeID": {"lte": 5}}},
                        "filter": {"term": {"PULocationID": 132}},
                        "should": {"range": {"PULocationID": {"gte": 100}}}
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("min_passenger_count")["value"],
            rollupAggRes.getValue("min_passenger_count")["value"],
        )

        // Boost query
        req =
            """
            {
                "size": 0,
                "query": {
                    "boosting": {
                       "positive": {"range": {"RatecodeID": {"gte": 1}}},
                       "negative": {"term": {"RatecodeID": 2}},
                       "negative_boost": 1.0
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("min_passenger_count")["value"],
            rollupAggRes.getValue("min_passenger_count")["value"],
        )

        // Const score query
        req =
            """
            {
                "size": 0,
                "query": {
                    "constant_score": {
                       "filter": {"range": {"RatecodeID": {"gt": 1}}},
                       "boost": 1.2
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("min_passenger_count")["value"],
            rollupAggRes.getValue("min_passenger_count")["value"],
        )

        // Dis max query
        req =
            """
            {
                "size": 0,
                "query": {
                    "dis_max": {
                        "queries": [
                           {"range": {"RatecodeID": {"gt": 1}}},
                           {"bool": {"filter": {"term": {"RatecodeID": 2}}}}
                        ],
                        "tie_breaker": 0.8
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("min_passenger_count")["value"],
            rollupAggRes.getValue("min_passenger_count")["value"],
        )

        // Match phrase query
        req =
            """
            {
                "size": 0,
                "query": {
                    "match_phrase": {
                        "RatecodeID": 1
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("min_passenger_count")["value"],
            rollupAggRes.getValue("min_passenger_count")["value"],
        )

        // Unsupported query
        req =
            """
            {
                "size": 0,
                "query": {
                    "match": {
                        "RatecodeID": 1
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        try {
            client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals(
                "Wrong error message",
                "The match query is currently not supported in rollups",
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"],
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // No valid job for rollup search
        req =
            """
            {
                "size": 0,
                "query": {
                    "bool": {
                        "must": {"range": {"RateCodeID": {"gt": 1}}},
                        "filter": {"term": {"timestamp": "2020-10-03T00:00:00.000Z"}}
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "total_amount"
                        }
                    }
                }
            }
            """.trimIndent()
        try {
            client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals(
                "Wrong error message",
                "Could not find a rollup job that can answer this query because [missing field RateCodeID, missing field timestamp, " +
                    "missing sum aggregation on total_amount]",
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"],
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // No query just aggregations
        req =
            """
            {
                "size": 0,
                "aggs": {
                    "sum": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    },
                    "min": {
                        "min": {
                            "field": "passenger_count"
                        }
                    },
                    "max": {
                        "max": {
                            "field": "passenger_count"
                        }
                    },
                    "avg": {
                        "avg": {
                            "field": "passenger_count"
                        }
                    },
                    "value_count": {
                        "value_count": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/source_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals("Source and rollup index did not return same max results", rawAggRes["max"]!!["value"], rollupAggRes["max"]!!["value"])
        assertEquals("Source and rollup index did not return same min results", rawAggRes["min"]!!["value"], rollupAggRes["min"]!!["value"])
        assertEquals("Source and rollup index did not return same sum results", rawAggRes["sum"]!!["value"], rollupAggRes["sum"]!!["value"])
        assertEquals("Source and rollup index did not return same value_count results", rawAggRes["value_count"]!!["value"], rollupAggRes["value_count"]!!["value"])
        assertEquals("Source and rollup index did not return same avg results", rawAggRes["avg"]!!["value"], rollupAggRes["avg"]!!["value"])

        // Invalid size in search - size > 0
        req =
            """
            {
                "size": 3,
                "aggs": { "sum": { "sum": { "field": "passenger_count" } } }
            }
            """.trimIndent()
        try {
            client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals(
                "Wrong error message",
                "Rollup search must have size explicitly set to 0, but found 3",
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"],
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // Invalid size in search - missing size
        req = """{ "aggs": { "sum": { "sum": { "field": "passenger_count" } } } }"""
        try {
            client().makeRequest("POST", "/target_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals(
                "Wrong error message",
                "Rollup search must have size explicitly set to 0, but found -1",
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"],
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test bucket and sub aggregations have correct values`() {
        generateNYCTaxiData("source_rollup_bucket_and_sub")
        val rollup =
            Rollup(
                id = "basic_term_query_rollup_bucket_and_sub",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = "source_rollup_bucket_and_sub",
                targetIndex = "target_rollup_bucket_and_sub",
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        // No query just bucket and sub metric aggregations
        val req =
            """
            {
                "size": 0,
                "aggs": {
                    "pickup_areas": {
                        "terms": { "field": "PULocationID", "size": 1000, "order": { "_key": "asc" } },
                        "aggs": {
                          "sum": { "sum": { "field": "passenger_count" } },
                          "min": { "min": { "field": "passenger_count" } },
                          "max": { "max": { "field": "passenger_count" } },
                          "avg": { "avg": { "field": "passenger_count" } },
                          "value_count": { "value_count": { "field": "passenger_count" } }
                        }
                    }
                }
            }
            """.trimIndent()
        val rawRes = client().makeRequest("POST", "/source_rollup_bucket_and_sub/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        val rollupRes = client().makeRequest("POST", "/target_rollup_bucket_and_sub/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        val rawAggBuckets = (rawRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)["pickup_areas"]!!["buckets"]!!
        val rollupAggBuckets = (rollupRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)["pickup_areas"]!!["buckets"]!!

        assertEquals("Different bucket sizes", rawAggBuckets.size, rollupAggBuckets.size)
        rawAggBuckets.forEachIndexed { idx, rawAggBucket ->
            val rollupAggBucket = rollupAggBuckets[idx]
            assertEquals(
                "The sum aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["sum"]!!["value"], rollupAggBucket["sum"]!!["value"],
            )
            assertEquals(
                "The max aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["max"]!!["value"], rollupAggBucket["max"]!!["value"],
            )
            assertEquals(
                "The min aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["min"]!!["value"], rollupAggBucket["min"]!!["value"],
            )
            assertEquals(
                "The value_count aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["value_count"]!!["value"], rollupAggBucket["value_count"]!!["value"],
            )
            assertEquals(
                "The avg aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["avg"]!!["value"], rollupAggBucket["avg"]!!["value"],
            )
        }
    }

    fun `test continuous rollup search`() {
        generateNYCTaxiData("source_continuous_rollup_search")
        val rollup =
            Rollup(
                id = "basic_term_query_continuous_rollup_search",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = "source_continuous_rollup_search",
                targetIndex = "target_continuous_rollup_search",
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = true,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "7d"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)

            // The test data does not have tpep_pickup_datetime going past "2019-01-01" so we can assume that
            // if the nextWindowStartTime is after 2019-01-02T00:00:00Z then all data has been rolled up
            assertTrue(
                "Rollup has not caught up yet, docs processed: ${rollupMetadata.stats.documentsProcessed}",
                rollupMetadata.continuous!!.nextWindowStartTime.isAfter(Instant.parse("2019-01-02T00:00:00Z")),
            )
        }

        refreshAllIndices()

        // Term query
        val req =
            """
            {
                "size": 0,
                "query": {
                    "term": {
                        "RatecodeID": 1
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "min": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        val rawRes = client().makeRequest("POST", "/source_continuous_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        val rollupRes = client().makeRequest("POST", "/target_continuous_rollup_search/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        val rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("min_passenger_count")["value"],
            rollupAggRes.getValue("min_passenger_count")["value"],
        )
    }

    fun `test rollup search all jobs`() {
        generateNYCTaxiData("source_rollup_search_all_jobs_1")
        generateNYCTaxiData("source_rollup_search_all_jobs_2")
        val targetIndex = "target_rollup_search_all_jobs"
        val rollupHourly =
            Rollup(
                id = "hourly_basic_term_query_rollup_search_all",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = "source_rollup_search_all_jobs_1",
                targetIndex = targetIndex,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupHourly)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupHourly.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        val rollupMinutely =
            Rollup(
                id = "minutely_basic_term_query_rollup_search_all",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = "source_rollup_search_all_jobs_2",
                targetIndex = targetIndex,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1m"),
                    Terms("RatecodeID", "RatecodeID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupMinutely)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupMinutely.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req =
            """
            {
                "size": 0,
                "query": {
                    "term": { "RatecodeID": 1 }
                },
                "aggs": {
                    "sum_passenger_count": { "sum": { "field": "passenger_count" } },
                    "max_passenger_count": { "max": { "field": "passenger_count" } },
                    "value_count_passenger_count": { "value_count": { "field": "passenger_count" } }
                }
            }
            """.trimIndent()
        val rawRes1 = client().makeRequest("POST", "/source_rollup_search_all_jobs_1/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes1.restStatus() == RestStatus.OK)
        val rawRes2 = client().makeRequest("POST", "/source_rollup_search_all_jobs_2/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes2.restStatus() == RestStatus.OK)
        val rollupResSingle = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupResSingle.restStatus() == RestStatus.OK)
        val rawAgg1Res = rawRes1.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rawAgg2Res = rawRes2.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rollupAggResSingle = rollupResSingle.asMap()["aggregations"] as Map<String, Map<String, Any>>

        // When the cluster setting to search all jobs is off, the aggregations will be the same for searching a single job as for searching both
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same max results",
            rawAgg1Res.getValue("max_passenger_count")["value"], rollupAggResSingle.getValue("max_passenger_count")["value"],
        )
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same sum results",
            rawAgg1Res.getValue("sum_passenger_count")["value"], rollupAggResSingle.getValue("sum_passenger_count")["value"],
        )
        val trueAggCount = rawAgg1Res.getValue("value_count_passenger_count")["value"] as Int + rawAgg2Res.getValue("value_count_passenger_count")["value"] as Int
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same value count results",
            rawAgg1Res.getValue("value_count_passenger_count")["value"], rollupAggResSingle.getValue("value_count_passenger_count")["value"],
        )

        val trueAggSum = rawAgg1Res.getValue("sum_passenger_count")["value"] as Double + rawAgg2Res.getValue("sum_passenger_count")["value"] as Double
        updateSearchAllJobsClusterSetting(true)

        val rollupResAll = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupResAll.restStatus() == RestStatus.OK)
        val rollupAggResAll = rollupResAll.asMap()["aggregations"] as Map<String, Map<String, Any>>

        // With search all jobs setting on, the sum, and value_count will now be equal to the sum of the single job search results
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same sum results",
            rawAgg1Res.getValue("max_passenger_count")["value"], rollupAggResAll.getValue("max_passenger_count")["value"],
        )
        assertEquals(
            "Searching rollup target index did not return the sum for all of the rollup jobs on the index",
            trueAggSum, rollupAggResAll.getValue("sum_passenger_count")["value"],
        )
        assertEquals(
            "Searching rollup target index did not return the value count for all of the rollup jobs on the index",
            trueAggCount, rollupAggResAll.getValue("value_count_passenger_count")["value"],
        )
    }

    fun `test rollup search multiple target indices successfully`() {
        val sourceIndex1 = "source_rollup_search_multi_jobs_1"
        val sourceIndex2 = "source_rollup_search_multi_jobs_2"
        generateNYCTaxiData(sourceIndex1)
        generateNYCTaxiData(sourceIndex2)
        val targetIndex1 = "target_rollup_search_multi_jobs1"
        val targetIndex2 = "target_rollup_search_multi_jobs2"
        val rollupHourly1 =
            Rollup(
                id = "hourly_basic_term_query_rollup_search_multi_1",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = sourceIndex1,
                targetIndex = targetIndex1,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupHourly1)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupHourly1.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        val rollupHourly2 =
            Rollup(
                id = "hourly_basic_term_query_rollup_search_multi_2",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = sourceIndex2,
                targetIndex = targetIndex2,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupHourly2)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupHourly2.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req =
            """
            {
                "size": 0,
                "query": {
                    "term": { "RatecodeID": 1 }
                },
                "aggs": {
                    "sum_passenger_count": { "sum": { "field": "passenger_count" } },
                    "max_passenger_count": { "max": { "field": "passenger_count" } },
                    "value_count_passenger_count": { "value_count": { "field": "passenger_count" } }
                }
            }
            """.trimIndent()
        val rawRes1 = client().makeRequest("POST", "/$sourceIndex1/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes1.restStatus() == RestStatus.OK)
        val rawRes2 = client().makeRequest("POST", "/$sourceIndex2/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes2.restStatus() == RestStatus.OK)
        val rollupResMulti = client().makeRequest("POST", "/$targetIndex1,$targetIndex2/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupResMulti.restStatus() == RestStatus.OK)
        val rawAgg1Res = rawRes1.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rawAgg2Res = rawRes2.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rollupAggResMulti = rollupResMulti.asMap()["aggregations"] as Map<String, Map<String, Any>>

        // When the cluster setting to search all jobs is off, the aggregations will be the same for searching a single job as for searching both
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same max results",
            rawAgg1Res.getValue("max_passenger_count")["value"], rollupAggResMulti.getValue("max_passenger_count")["value"],
        )
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same sum results",
            rawAgg1Res.getValue("sum_passenger_count")["value"], rollupAggResMulti.getValue("sum_passenger_count")["value"],
        )
        val trueAggCount = rawAgg1Res.getValue("value_count_passenger_count")["value"] as Int + rawAgg2Res.getValue("value_count_passenger_count")["value"] as Int
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same value count results",
            rawAgg1Res.getValue("value_count_passenger_count")["value"], rollupAggResMulti.getValue("value_count_passenger_count")["value"],
        )

        val trueAggSum = rawAgg1Res.getValue("sum_passenger_count")["value"] as Double + rawAgg2Res.getValue("sum_passenger_count")["value"] as Double
        updateSearchAllJobsClusterSetting(true)

        val rollupResAll = client().makeRequest("POST", "/$targetIndex1,$targetIndex2/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupResAll.restStatus() == RestStatus.OK)
        val rollupAggResAll = rollupResAll.asMap()["aggregations"] as Map<String, Map<String, Any>>

        // With search all jobs setting on, the sum, and value_count will now be equal to the sum of the single job search results
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same sum results",
            rawAgg1Res.getValue("max_passenger_count")["value"], rollupAggResAll.getValue("max_passenger_count")["value"],
        )
        assertEquals(
            "Searching rollup target index did not return the sum for all of the rollup jobs on the index",
            trueAggSum, rollupAggResAll.getValue("sum_passenger_count")["value"],
        )
        assertEquals(
            "Searching rollup target index did not return the value count for all of the rollup jobs on the index",
            trueAggCount, rollupAggResAll.getValue("value_count_passenger_count")["value"],
        )
    }

    fun `test rollup search multiple target indices failed`() {
        val sourceIndex1 = "source_rollup_search_multi_failed_1"
        val sourceIndex2 = "source_rollup_search_multi_failed_2"
        generateNYCTaxiData(sourceIndex1)
        generateNYCTaxiData(sourceIndex2)
        val targetIndex1 = "target_rollup_search_multi_failed_jobs1"
        val targetIndex2 = "target_rollup_search_multi_failed_jobs2"
        val rollupJob1 =
            Rollup(
                id = "hourly_basic_term_query_rollup_search_failed_1",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = sourceIndex1,
                targetIndex = targetIndex1,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("VendorID", "VendorID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "fare_amount", targetField = "fare_amount",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "improvement_surcharge", targetField = "improvement_surcharge", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupJob1)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupJob1.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        val rollupJob2 =
            Rollup(
                id = "hourly_basic_term_query_rollup_search_failed_2",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = sourceIndex2,
                targetIndex = targetIndex2,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_dropoff_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupJob2)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupJob2.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req =
            """
            {
                "size": 0,
                "query": {
                    "term": { "RatecodeID": 1 }
                },
                "aggs": {
                    "sum_passenger_count": { "sum": { "field": "passenger_count" } },
                    "max_passenger_count": { "max": { "field": "passenger_count" } },
                    "value_count_passenger_count": { "value_count": { "field": "passenger_count" } }
                }
            }
            """.trimIndent()
        // Search 1 non-rollup index and 1 rollup
        val searchResult1 = client().makeRequest("POST", "/$sourceIndex2,$targetIndex2/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(searchResult1.restStatus() == RestStatus.OK)
        val failures = extractFailuresFromSearchResponse(searchResult1)
        assertNotNull(failures)
        assertEquals(1, failures?.size)
        assertEquals(
            "Searching multiple indices where one is rollup and other is not, didn't return failure",
            "illegal_argument_exception", failures?.get(0)?.get("type") ?: "Didn't find failure type in search response",
        )
        assertEquals(
            "Searching multiple indices where one is rollup and other is not, didn't return failure",
            "Not all indices have rollup job", failures?.get(0)?.get("reason") ?: "Didn't find failure reason in search response",
        )

        // Updating to allow searching on non-rollup and rolled-up index together
        updateSearchRawRollupClusterSetting(true)
        val rawRes1 = client().makeRequest("POST", "/$sourceIndex2/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes1.restStatus() == RestStatus.OK)
        val rawRes2 = client().makeRequest("POST", "/$targetIndex2/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes2.restStatus() == RestStatus.OK)
        val searchResult2 = client().makeRequest("POST", "/$sourceIndex2,$targetIndex2/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(searchResult2.restStatus() == RestStatus.OK)
        val rawAgg1Res = rawRes1.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rawAgg2Res = rawRes2.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rollupAggResMulti = searchResult2.asMap()["aggregations"] as Map<String, Map<String, Any>>

        val trueAggCount = rawAgg1Res.getValue("value_count_passenger_count")["value"] as Int + rawAgg2Res.getValue("value_count_passenger_count")["value"] as Int
        val trueAggSum = rawAgg1Res.getValue("sum_passenger_count")["value"] as Double + rawAgg2Res.getValue("sum_passenger_count")["value"] as Double

        assertEquals(
            "Searching single raw source index and rollup target index did not return the same sum results",
            rawAgg1Res.getValue("max_passenger_count")["value"], rollupAggResMulti.getValue("max_passenger_count")["value"],
        )
        assertEquals(
            "Searching rollup target index did not return the sum for all of the rollup jobs on the index",
            trueAggSum, rollupAggResMulti.getValue("sum_passenger_count")["value"],
        )
        assertEquals(
            "Searching rollup target index did not return the value count for all of the rollup jobs on the index",
            trueAggCount, rollupAggResMulti.getValue("value_count_passenger_count")["value"],
        )

        // Search 2 rollups with different mappings
        try {
            client().makeRequest(
                "POST",
                "/$targetIndex1,$targetIndex2/_search",
                emptyMap(),
                StringEntity(req, ContentType.APPLICATION_JSON),
            )
        } catch (e: ResponseException) {
            assertEquals(
                "Searching multiple rollup indices which weren't created by same rollup job, didn't return failure",
                "Could not find a rollup job that can answer this query because [missing field RatecodeID, missing field passenger_count]",
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"],
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test roll up search query_string query`() {
        val sourceIndex = "source_rollup_search_qsq_1"
        val targetIndex = "target_rollup_qsq_search_1"

        createSampleIndexForQSQTest(sourceIndex)

        val rollup =
            Rollup(
                id = "basic_query_string_query_rollup_search111",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = sourceIndex,
                targetIndex = targetIndex,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "event_ts", fixedInterval = "1h"),
                    Terms("state", "state"),
                    Terms("state_ext", "state_ext"),
                    Terms("state_ext2", "state_ext2"),
                    Terms("state_ordinal", "state_ordinal"),
                    Terms("abc test", "abc test"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "earnings", targetField = "earnings",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        // Term query
        var req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:TX AND state_ext:CA AND 0",
                        "default_field": "state_ordinal"
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        var rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        var rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        var rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        var rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )

        // Fuzzy query
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:TX~2"
                    }
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )
        // Prefix query
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:T*"
                    }
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )
        // Regex query
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:/[A-Z]T/"
                    }
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )
        // Range query
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state_ordinal:[0 TO 10]"
                    }
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )
        // Query with field prefix
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "123",
                        "default_field":"abc*"
                    }
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )

        // Using ALL_MATCH_PATTERN for default_field but rollup job didn't include all fields
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:TX AND state_ext:CA AND 12345",
                        "default_field": "*"
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        try {
            client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        } catch (e: ResponseException) {
            assertTrue(
                e.message?.contains(
                    "[missing terms grouping on earnings, missing terms grouping on event_ts, missing field test.vvv, missing field test.fff]",
                ) ?: false,
            )
        }

        // Using ALL_MATCH_PATTERN in one of fields in "fields" array but rollup job didn't include all fields
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:TX AND state_ext:CA AND 12345",
                        "fields": ["state", "*"]
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        try {
            client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        } catch (e: ResponseException) {
            assertTrue(
                e.message?.contains(
                    "[missing terms grouping on earnings, missing terms grouping on event_ts, missing field test.vvv, missing field test.fff]",
                ) ?: false,
            )
        }

        // field from "fields" list is missing in rollup
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:TX AND state_ext:CA AND 12345",
                        "fields": ["test.fff"]
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        try {
            client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        } catch (e: ResponseException) {
            assertTrue(e.message?.contains("[missing field test.fff]") ?: false)
        }

        // no fields or default_field present. Fallback on index setting [index.query.default_field] default value: "*"
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:TX AND state_ext:CA AND 12345"
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        try {
            client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        } catch (e: ResponseException) {
            assertTrue(
                e.message?.contains(
                    "[missing terms grouping on earnings, missing terms grouping on event_ts, missing field test.vvv, missing field test.fff]",
                ) ?: false,
            )
        }

        // fallback on index settings index.query.default_field:state_ordinal
        client().makeRequest(
            "PUT", "$sourceIndex/_settings",
            StringEntity(
                """
                {
                    "index": {
                        "query": {
                            "default_field":"state_ordinal"
                        }
                    }
                }
                """.trimIndent(),
                ContentType.APPLICATION_JSON,
            ),
        )
        //
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:TX AND state_ext:CA AND 7"
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )

        // prefix pattern in "default_field" field
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "TX AND CA",
                        "default_field": "state_e*"
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )

        // field with space in query:
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "abc\\ test:123"
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )

        // _exists_:field
        req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "_exists_:abc\\ test"
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )
    }

    fun `test roll up search query_string query invalid query`() {
        val sourceIndex = "source_rollup_search_qsq_2"
        val targetIndex = "target_rollup_qsq_search_2"
        generateNYCTaxiData(sourceIndex)
        val rollup =
            Rollup(
                id = "basic_query_string_query_rollup_search_2",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = sourceIndex,
                targetIndex = targetIndex,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        // Invalid query
        var req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "::!invalid+-+-::query:::"
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        try {
            client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            fail("search should've failed due to incorrect query")
        } catch (e: ResponseException) {
            assertTrue("The query_string query wasn't invalid", e.message!!.contains("Failed to parse query"))
        }
    }

    fun `test roll up search query_string query unknown field`() {
        val sourceIndex = "source_rollup_search_qsq"
        val targetIndex = "target_rollup_qsq_search"
        generateNYCTaxiData(sourceIndex)
        val rollup =
            Rollup(
                id = "basic_query_string_query_rollup_search",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = sourceIndex,
                targetIndex = targetIndex,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        // Term query
        var req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "RatecodeID:>=1 AND unknown_field:<=10"
                    }
                },
                "aggs": {
                    "min_passenger_count": {
                        "sum": {
                            "field": "passenger_count"
                        }
                    }
                }
            }
            """.trimIndent()
        try {
            client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            fail("search should've failed due to incorrect query")
        } catch (e: ResponseException) {
            assertTrue("The query_string query field check failed!", e.message!!.contains("Could not find a rollup job that can answer this query because [missing field unknown_field]"))
        }
    }

    fun `test roll up search query_string query with index pattern as source`() {
        val sourceIndex = "source_111_rollup_search_qsq_98243"
        val targetIndex = "target_rollup_qsq_search_98243"

        createSampleIndexForQSQTest(sourceIndex)

        val rollup =
            Rollup(
                id = "basic_query_string_query_rollup_search98243",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = "source_111*",
                targetIndex = targetIndex,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "event_ts", fixedInterval = "1h"),
                    Terms("state", "state"),
                    Terms("state_ext", "state_ext"),
                    Terms("state_ext2", "state_ext2"),
                    Terms("state_ordinal", "state_ordinal"),
                    Terms("abc test", "abc test"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "earnings", targetField = "earnings",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        // Term query
        var req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:TX AND state_ext:CA AND 0",
                        "default_field": "state_ordinal"
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        var rawRes = client().makeRequest("POST", "/$sourceIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)
        var rollupRes = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupRes.restStatus() == RestStatus.OK)
        var rawAggRes = rawRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        var rollupAggRes = rollupRes.asMap()["aggregations"] as Map<String, Map<String, Any>>
        assertEquals(
            "Source and rollup index did not return same min results",
            rawAggRes.getValue("earnings_total")["value"],
            rollupAggRes.getValue("earnings_total")["value"],
        )
    }

    fun `test roll up search query_string query with index pattern as source deleted`() {
        val sourceIndex = "source_999_rollup_search_qsq_982439"
        val targetIndex = "target_rollup_qsq_search_982439"

        createSampleIndexForQSQTest(sourceIndex)

        val rollup =
            Rollup(
                id = "basic_query_string_query_rollup_search982499",
                enabled = true,
                schemaVersion = 1L,
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                jobLastUpdatedTime = Instant.now(),
                jobEnabledTime = Instant.now(),
                description = "basic search test",
                sourceIndex = "source_999*",
                targetIndex = targetIndex,
                metadataID = null,
                roles = emptyList(),
                pageSize = 10,
                delay = 0,
                continuous = false,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "event_ts", fixedInterval = "1h"),
                    Terms("state", "state"),
                    Terms("state_ext", "state_ext"),
                    Terms("state_ext2", "state_ext2"),
                    Terms("state_ordinal", "state_ordinal"),
                    Terms("abc test", "abc test"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "earnings", targetField = "earnings",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                ),
            ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollup)

        waitFor {
            val rollupJob = getRollup(rollupId = rollup.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        deleteIndex(sourceIndex)

        // Term query
        var req =
            """
            {
                "size": 0,
                "query": {
                    "query_string": {
                        "query": "state:TX AND state_ext:CA AND 0",
                        "default_field": "state_ordinal"
                    }
                    
                },
                "aggs": {
                    "earnings_total": {
                        "sum": {
                            "field": "earnings"
                        }
                    }
                }
            }
            """.trimIndent()
        try {
            client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            fail("Failure was expected when searching rollup index using qsq query when sourceIndex does not exist!")
        } catch (e: ResponseException) {
            Assert.assertTrue(e.message!!.contains("Can't parse query_string query without sourceIndex mappings!"))
        }
    }
}
