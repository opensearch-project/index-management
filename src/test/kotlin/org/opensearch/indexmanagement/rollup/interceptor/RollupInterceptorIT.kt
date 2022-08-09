/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.client.ResponseException
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
class RollupInterceptorIT : RollupRestTestCase() {

    fun `test roll up search`() {
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
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                ),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
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

        // Term query
        var req = """
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
            rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Terms query
        req = """
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
            rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Range query
        req = """
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
            rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Bool query
        req = """
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
            rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Boost query
        req = """
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
            rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Const score query
        req = """
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
            rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Dis max query
        req = """
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
            rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Match phrase query
        req = """
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
            rollupAggRes.getValue("min_passenger_count")["value"]
        )

        // Unsupported query
        req = """
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
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"]
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // No valid job for rollup search
        req = """
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
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"]
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // No query just aggregations
        req = """
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
        req = """
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
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"]
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
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"]
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test bucket and sub aggregations have correct values`() {
        generateNYCTaxiData("source_rollup_bucket_and_sub")
        val rollup = Rollup(
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
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                ),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
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

        // No query just bucket and sub metric aggregations
        val req = """
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
                rawAggBucket["sum"]!!["value"], rollupAggBucket["sum"]!!["value"]
            )
            assertEquals(
                "The max aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["max"]!!["value"], rollupAggBucket["max"]!!["value"]
            )
            assertEquals(
                "The min aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["min"]!!["value"], rollupAggBucket["min"]!!["value"]
            )
            assertEquals(
                "The value_count aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["value_count"]!!["value"], rollupAggBucket["value_count"]!!["value"]
            )
            assertEquals(
                "The avg aggregation had a different value raw[$rawAggBucket] rollup[$rollupAggBucket]",
                rawAggBucket["avg"]!!["value"], rollupAggBucket["avg"]!!["value"]
            )
        }
    }

    fun `test continuous rollup search`() {
        generateNYCTaxiData("source_continuous_rollup_search")
        val rollup = Rollup(
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
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "7d"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                ),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
            )
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
                rollupMetadata.continuous!!.nextWindowStartTime.isAfter(Instant.parse("2019-01-02T00:00:00Z"))
            )
        }

        refreshAllIndices()

        // Term query
        val req = """
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
            rollupAggRes.getValue("min_passenger_count")["value"]
        )
    }

    fun `test rollup search all jobs`() {
        generateNYCTaxiData("source_rollup_search_all_jobs_1")
        generateNYCTaxiData("source_rollup_search_all_jobs_2")
        val targetIndex = "target_rollup_search_all_jobs"
        val rollupHourly = Rollup(
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
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                ),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupHourly)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupHourly.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        val rollupMinutely = Rollup(
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
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1m"),
                Terms("RatecodeID", "RatecodeID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                ),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupMinutely)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupMinutely.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req = """
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
            rawAgg1Res.getValue("max_passenger_count")["value"], rollupAggResSingle.getValue("max_passenger_count")["value"]
        )
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same sum results",
            rawAgg1Res.getValue("sum_passenger_count")["value"], rollupAggResSingle.getValue("sum_passenger_count")["value"]
        )
        val trueAggCount = rawAgg1Res.getValue("value_count_passenger_count")["value"] as Int + rawAgg2Res.getValue("value_count_passenger_count")["value"] as Int
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same value count results",
            rawAgg1Res.getValue("value_count_passenger_count")["value"], rollupAggResSingle.getValue("value_count_passenger_count")["value"]
        )

        val trueAggSum = rawAgg1Res.getValue("sum_passenger_count")["value"] as Double + rawAgg2Res.getValue("sum_passenger_count")["value"] as Double
        updateSearchAllJobsClusterSetting(true)

        val rollupResAll = client().makeRequest("POST", "/$targetIndex/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupResAll.restStatus() == RestStatus.OK)
        val rollupAggResAll = rollupResAll.asMap()["aggregations"] as Map<String, Map<String, Any>>

        // With search all jobs setting on, the sum, and value_count will now be equal to the sum of the single job search results
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same sum results",
            rawAgg1Res.getValue("max_passenger_count")["value"], rollupAggResAll.getValue("max_passenger_count")["value"]
        )
        assertEquals(
            "Searching rollup target index did not return the sum for all of the rollup jobs on the index",
            trueAggSum, rollupAggResAll.getValue("sum_passenger_count")["value"]
        )
        assertEquals(
            "Searching rollup target index did not return the value count for all of the rollup jobs on the index",
            trueAggCount, rollupAggResAll.getValue("value_count_passenger_count")["value"]
        )
    }

    fun `test rollup search multiple target indices successfully`() {
        val sourceIndex1 = "source_rollup_search_multi_jobs_1"
        val sourceIndex2 = "source_rollup_search_multi_jobs_2"
        generateNYCTaxiData(sourceIndex1)
        generateNYCTaxiData(sourceIndex2)
        val targetIndex1 = "target_rollup_search_multi_jobs1"
        val targetIndex2 = "target_rollup_search_multi_jobs2"
        val rollupHourly1 = Rollup(
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
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                ),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupHourly1)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupHourly1.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        val rollupHourly2 = Rollup(
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
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                ),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupHourly2)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupHourly2.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req = """
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
            rawAgg1Res.getValue("max_passenger_count")["value"], rollupAggResMulti.getValue("max_passenger_count")["value"]
        )
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same sum results",
            rawAgg1Res.getValue("sum_passenger_count")["value"], rollupAggResMulti.getValue("sum_passenger_count")["value"]
        )
        val trueAggCount = rawAgg1Res.getValue("value_count_passenger_count")["value"] as Int + rawAgg2Res.getValue("value_count_passenger_count")["value"] as Int
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same value count results",
            rawAgg1Res.getValue("value_count_passenger_count")["value"], rollupAggResMulti.getValue("value_count_passenger_count")["value"]
        )

        val trueAggSum = rawAgg1Res.getValue("sum_passenger_count")["value"] as Double + rawAgg2Res.getValue("sum_passenger_count")["value"] as Double
        updateSearchAllJobsClusterSetting(true)

        val rollupResAll = client().makeRequest("POST", "/$targetIndex1,$targetIndex2/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
        assertTrue(rollupResAll.restStatus() == RestStatus.OK)
        val rollupAggResAll = rollupResAll.asMap()["aggregations"] as Map<String, Map<String, Any>>

        // With search all jobs setting on, the sum, and value_count will now be equal to the sum of the single job search results
        assertEquals(
            "Searching single rollup job and rollup target index did not return the same sum results",
            rawAgg1Res.getValue("max_passenger_count")["value"], rollupAggResAll.getValue("max_passenger_count")["value"]
        )
        assertEquals(
            "Searching rollup target index did not return the sum for all of the rollup jobs on the index",
            trueAggSum, rollupAggResAll.getValue("sum_passenger_count")["value"]
        )
        assertEquals(
            "Searching rollup target index did not return the value count for all of the rollup jobs on the index",
            trueAggCount, rollupAggResAll.getValue("value_count_passenger_count")["value"]
        )
    }

    fun `test rollup search multiple target indices failed`() {
        val sourceIndex1 = "source_rollup_search_multi_failed_1"
        val sourceIndex2 = "source_rollup_search_multi_failed_2"
        generateNYCTaxiData(sourceIndex1)
        generateNYCTaxiData(sourceIndex2)
        val targetIndex1 = "target_rollup_search_multi_failed_jobs1"
        val targetIndex2 = "target_rollup_search_multi_failed_jobs2"
        val rollupJob1 = Rollup(
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
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("VendorID", "VendorID"),
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "fare_amount", targetField = "fare_amount",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                ),
                RollupMetrics(sourceField = "improvement_surcharge", targetField = "improvement_surcharge", metrics = listOf(Max(), Min()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupJob1)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupJob1.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        val rollupJob2 = Rollup(
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
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_dropoff_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                ),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
            )
        ).let { createRollup(it, it.id) }

        updateRollupStartTime(rollupJob2)

        waitFor {
            val rollupJob = getRollup(rollupId = rollupJob2.id)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        refreshAllIndices()

        val req = """
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
            "illegal_argument_exception", failures?.get(0)?.get("type") ?: "Didn't find failure type in search response"

        )
        assertEquals(
            "Searching multiple indices where one is rollup and other is not, didn't return failure",
            "Not all indices have rollup job", failures?.get(0)?.get("reason") ?: "Didn't find failure reason in search response"
        )

        // Search 2 rollups with different mappings
        try {
            client().makeRequest(
                "POST",
                "/$targetIndex1,$targetIndex2/_search",
                emptyMap(),
                StringEntity(req, ContentType.APPLICATION_JSON)
            )
        } catch (e: ResponseException) {
            assertEquals(
                "Searching multiple rollup indices which weren't created by same rollup job, didn't return failure",
                "Could not find a rollup job that can answer this query because [missing field RatecodeID, missing field passenger_count]",
                (e.response.asMap() as Map<String, Map<String, Map<String, String>>>)["error"]!!["caused_by"]!!["reason"]
            )
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }
}
