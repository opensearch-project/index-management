/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Histogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestRequest
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder
import org.opensearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder
import java.lang.Integer.min
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.assertFailsWith

class TransformRunnerIT : TransformRestTestCase() {

    fun `test transform`() {
        validateSourceIndex("transform-source-index")

        val transform = Transform(
            id = "id_1",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-index",
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
            ),
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor { assertTrue("Target transform index was not created", indexExists(transform.targetIndex)) }

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
            transformMetadata
        }

        assertEquals("More than expected pages processed", 3L, metadata.stats.pagesProcessed)
        assertEquals("More than expected documents indexed", 2L, metadata.stats.documentsIndexed)
        assertEquals("More than expected documents processed", 5000L, metadata.stats.documentsProcessed)
        // In some cases it seems that these times are less than 1ms - which causes fails on ubuntu instances (at least that was detected)
        assertTrue("Doesn't capture indexed time", metadata.stats.indexTimeInMillis >= 0)
        assertTrue("Didn't capture search time", metadata.stats.searchTimeInMillis >= 0)
    }

    fun `test transform with data filter`() {
        validateSourceIndex("transform-source-index")

        val transform = Transform(
            id = "id_2",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-index",
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
            ),
            dataSelectionQuery = TermQueryBuilder("store_and_fwd_flag", "N"),
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
            transformMetadata
        }

        assertEquals("More than expected pages processed", 2L, metadata.stats.pagesProcessed)
        assertEquals("More than expected documents indexed", 1L, metadata.stats.documentsIndexed)
        assertEquals("More than expected documents processed", 4977L, metadata.stats.documentsProcessed)
        // In some cases it seems that these times are less than 1ms - which causes fails on ubuntu instances (at least that was detected)
        assertTrue("Doesn't capture indexed time", metadata.stats.indexTimeInMillis >= 0)
        assertTrue("Didn't capture search time", metadata.stats.searchTimeInMillis >= 0)
    }

    fun `test invalid transform`() {
        // With invalid mapping
        val transform = randomTransform().copy(enabled = true, jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES))
        createTransform(transform, transform.id)
        deleteIndex(transform.sourceIndex)

        updateTransformStartTime(transform)

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not failed", TransformMetadata.Status.FAILED, transformMetadata.status)
            transformMetadata
        }

        assertTrue("Expected failure message to be present", !metadata.failureReason.isNullOrBlank())
    }

    fun `test transform with aggregations`() {
        validateSourceIndex("transform-source-index")

        val aggregatorFactories = AggregatorFactories.builder()
        aggregatorFactories.addAggregator(AggregationBuilders.sum("revenue").field("total_amount"))
        aggregatorFactories.addAggregator(AggregationBuilders.max("min_fare").field("fare_amount"))
        aggregatorFactories.addAggregator(AggregationBuilders.min("max_fare").field("fare_amount"))
        aggregatorFactories.addAggregator(AggregationBuilders.avg("avg_fare").field("fare_amount"))
        aggregatorFactories.addAggregator(AggregationBuilders.count("count").field("orderID"))
        aggregatorFactories.addAggregator(AggregationBuilders.percentiles("passenger_distribution").percentiles(90.0, 95.0).field("passenger_count"))
        aggregatorFactories.addAggregator(
            ScriptedMetricAggregationBuilder("average_revenue_per_passenger_per_trip")
                .initScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "state.count = 0; state.sum = 0;", emptyMap()))
                .mapScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "state.sum += doc[\"total_amount\"].value; state.count += doc[\"passenger_count\"].value",
                        emptyMap(),
                    ),
                )
                .combineScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "def d = new long[2]; d[0] = state.sum; d[1] = state.count; return d",
                        emptyMap(),
                    ),
                )
                .reduceScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "double sum = 0; double count = 0; for (a in states) { sum += a[0]; count += a[1]; } return sum/count",
                        emptyMap(),
                    ),
                ),
        )

        val transform = Transform(
            id = "id_4",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-index",
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
            ),
            aggregations = aggregatorFactories,
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
            transformMetadata
        }

        assertEquals("More than expected pages processed", 3L, metadata.stats.pagesProcessed)
        assertEquals("More than expected documents indexed", 2L, metadata.stats.documentsIndexed)
        assertEquals("More than expected documents processed", 5000L, metadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", metadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", metadata.stats.searchTimeInMillis > 0)
    }

    @Suppress("UNCHECKED_CAST")
    fun `test transform target index _doc_count against the source index _doc_count`() {
        val sourceIdxTestName = "source_idx_test"
        val targetIdxTestName = "target_idx_test"

        val storeAndForwardTerm = "store_and_fwd_flag"
        val fareAmount = "fare_amount"
        val avgAmountPerFlag = "avg_amount_per_store_flag"

        validateSourceIndex(sourceIdxTestName)

        val transform = Transform(
            id = "id_13",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform doc values must be the same",
            metadataId = null,
            sourceIndex = sourceIdxTestName,
            targetIndex = targetIdxTestName,
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = storeAndForwardTerm, targetField = storeAndForwardTerm),
            ),
            aggregations = AggregatorFactories.builder().addAggregator(AggregationBuilders.avg(fareAmount).field(fareAmount)),
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor {
            assertTrue("Target transform index was not created", indexExists(transform.targetIndex))
        }

        waitFor {
            val transformJob = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", transformJob.metadataId)
            val transformMetadata = getTransformMetadata(transformJob.metadataId!!)
            assertEquals("Transform is not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)

            val req = """
            {
                "size": 0,
                "aggs": {
                    "$avgAmountPerFlag": {
                        "terms": {
                            "field": "$storeAndForwardTerm", "order": { "_key": "asc" }
                        },
                        "aggs": {
                          "avg": { "avg": { "field": "$fareAmount" } } }
                    }
                }
            }
            """.trimIndent()

            var rawRes = client().makeRequest(RestRequest.Method.POST.name, "/$sourceIdxTestName/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            assertTrue(rawRes.restStatus() == RestStatus.OK)

            var transformRes = client().makeRequest(RestRequest.Method.POST.name, "/$targetIdxTestName/_search", emptyMap(), StringEntity(req, ContentType.APPLICATION_JSON))
            assertTrue(transformRes.restStatus() == RestStatus.OK)

            val rawAggBuckets = (rawRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)[avgAmountPerFlag]!!["buckets"]!!
            val transformAggBuckets = (transformRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)[avgAmountPerFlag]!!["buckets"]!!

            assertEquals("Different bucket sizes", rawAggBuckets.size, transformAggBuckets.size)
            rawAggBuckets.forEachIndexed { idx, rawAggBucket ->
                val transformAggBucket = transformAggBuckets[idx]
                assertEquals(
                    "The doc_count had a different value raw[$rawAggBucket] transform[$transformAggBucket]",
                    rawAggBucket["doc_count"]!!, transformAggBucket["doc_count"]!!,
                )
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test transform term aggregation on date field generate target mapping same as source mapping for date field`() {
        val sourceIdxTestName = "source_idx_test_14"
        val targetIdxTestName = "target_idx_test_14"

        val pickupDateTime = "tpep_pickup_datetime"

        val fareAmount = "fare_amount"

        validateSourceIndex(sourceIdxTestName)

        val transform = Transform(
            id = "id_14",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform doc values must be the same",
            metadataId = null,
            sourceIndex = sourceIdxTestName,
            targetIndex = targetIdxTestName,
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = pickupDateTime, targetField = pickupDateTime),
            ),
            aggregations = AggregatorFactories.builder().addAggregator(AggregationBuilders.avg(fareAmount).field(fareAmount)),
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor {
            assertTrue("Target transform index was not created", indexExists(transform.targetIndex))
        }

        waitFor(Instant.ofEpochSecond(180)) {
            val transformJob = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", transformJob.metadataId)
            val transformMetadata = getTransformMetadata(transformJob.metadataId!!)
            assertEquals("Transform is not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
        }

        val sourceIndexMapping = client().makeRequest("GET", "/$sourceIdxTestName/_mapping")
        val sourceIndexParserMap = createParser(XContentType.JSON.xContent(), sourceIndexMapping.entity.content).map() as Map<String, Map<String, Any>>
        val targetIndexMapping = client().makeRequest("GET", "/$targetIdxTestName/_mapping")
        val targetIndexParserMap = createParser(XContentType.JSON.xContent(), targetIndexMapping.entity.content).map() as Map<String, Map<String, Any>>

        val sourcePickupDate = (((sourceIndexParserMap[sourceIdxTestName]?.get("mappings") as Map<String, Any>)["properties"] as Map<String, Any>)["tpep_pickup_datetime"] as Map<String, Any>)["type"]
        val targetPickupDate = (((targetIndexParserMap[targetIdxTestName]?.get("mappings") as Map<String, Any>)["properties"] as Map<String, Any>)["tpep_pickup_datetime"] as Map<String, Any>)["type"]

        assertEquals(sourcePickupDate, targetPickupDate)

        val pickupDateTimeTerm = "pickupDateTerm14"

        val request = """
            {
                "size": 0,
                "aggs": {
                    "$pickupDateTimeTerm": {
                        "terms": {
                            "field": "$pickupDateTime", "order": { "_key": "asc" }
                        },
                        "aggs": {
                          "avgFareAmount": { "avg": { "field": "$fareAmount" } } }
                    }
                }
            }
            """

        var rawRes = client().makeRequest(RestRequest.Method.POST.name, "/$sourceIdxTestName/_search", emptyMap(), StringEntity(request, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)

        var transformRes = client().makeRequest(RestRequest.Method.POST.name, "/$targetIdxTestName/_search", emptyMap(), StringEntity(request, ContentType.APPLICATION_JSON))
        assertTrue(transformRes.restStatus() == RestStatus.OK)

        val rawAggBuckets = (rawRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)[pickupDateTimeTerm]!!["buckets"]!!
        val transformAggBuckets = (transformRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)[pickupDateTimeTerm]!!["buckets"]!!

        assertEquals("Different bucket sizes", rawAggBuckets.size, transformAggBuckets.size)

        // Verify the values of keys and metrics in all buckets
        for (i in rawAggBuckets.indices) {
            assertEquals("Term pickup date bucket keys are not the same", rawAggBuckets[i]["key"], transformAggBuckets[i]["key"])
            assertEquals("Avg fare amounts are not the same", rawAggBuckets[i]["avgFareAmount"], transformAggBuckets[i]["avgFareAmount"])
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test transform max aggregation on date field verify search request term aggregation on store_and_fwd_flag field`() {
        val sourceIdxTestName = "source_idx_test_15"
        val targetIdxTestName = "target_idx_test_15"

        val storeAndForward = "store_and_fwd_flag"
        val pickupDateTime = "tpep_pickup_datetime"
        val fareAmount = "fare_amount"

        validateSourceIndex(sourceIdxTestName)

        val avgFareAmountAgg = AggregationBuilders.avg(fareAmount).field(fareAmount)
        val maxDateAggBuilder = AggregationBuilders.max(pickupDateTime).field(pickupDateTime)

        val transform = Transform(
            id = "id_15",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform doc values must be the same",
            metadataId = null,
            sourceIndex = sourceIdxTestName,
            targetIndex = targetIdxTestName,
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = storeAndForward, targetField = storeAndForward),
            ),
            aggregations = AggregatorFactories.builder().addAggregator(avgFareAmountAgg).addAggregator(maxDateAggBuilder),
        ).let { createTransform(it, it.id) }
        updateTransformStartTime(transform)

        waitFor {
            assertTrue("Target transform index was not created", indexExists(transform.targetIndex))
        }

        waitFor(timeout = Instant.ofEpochSecond(30)) {
            val transformJob = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", transformJob.metadataId)
            val transformMetadata = getTransformMetadata(transformJob.metadataId!!)
            assertEquals("Transform is not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
        }

        val sourceIndexMapping = client().makeRequest("GET", "/$sourceIdxTestName/_mapping")
        val sourceIndexParserMap = createParser(XContentType.JSON.xContent(), sourceIndexMapping.entity.content).map() as Map<String, Map<String, Any>>
        val targetIndexMapping = client().makeRequest("GET", "/$targetIdxTestName/_mapping")
        val targetIndexParserMap = createParser(XContentType.JSON.xContent(), targetIndexMapping.entity.content).map() as Map<String, Map<String, Any>>

        val sourcePickupDate = (((sourceIndexParserMap[sourceIdxTestName]?.get("mappings") as Map<String, Any>)["properties"] as Map<String, Any>)[pickupDateTime] as Map<String, Any>)["type"]
        val targetPickupDate = (((targetIndexParserMap[targetIdxTestName]?.get("mappings") as Map<String, Any>)["properties"] as Map<String, Any>)[pickupDateTime] as Map<String, Any>)["type"]

        assertEquals("date", targetPickupDate)
        assertEquals(sourcePickupDate, targetPickupDate)

        waitFor(Instant.ofEpochSecond(30)) {
            val storeAndForwardTerm = "storeAndForwardTerm"
            val request = """
            {
                "size": 0,
                "aggs": {
                    "$storeAndForwardTerm": {
                        "terms": {
                            "field": "$storeAndForward", "order": { "_key": "asc" }
                        },
                        "aggs": {
                           "$fareAmount": { "avg": { "field": "$fareAmount" } },
                           "$pickupDateTime": {"max": {"field": "$pickupDateTime"}}
                        }
                    }
                }
            }
            """

            var rawRes = client().makeRequest(RestRequest.Method.POST.name, "/$sourceIdxTestName/_search", emptyMap(), StringEntity(request, ContentType.APPLICATION_JSON))
            assertTrue(rawRes.restStatus() == RestStatus.OK)

            var transformRes = client().makeRequest(RestRequest.Method.POST.name, "/$targetIdxTestName/_search", emptyMap(), StringEntity(request, ContentType.APPLICATION_JSON))
            assertTrue(transformRes.restStatus() == RestStatus.OK)

            val rawAggBuckets = (rawRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)[storeAndForwardTerm]!!["buckets"]!!
            val transformAggBuckets = (transformRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)[storeAndForwardTerm]!!["buckets"]!!

            assertEquals("Different bucket sizes", rawAggBuckets.size, transformAggBuckets.size)

            for (i in rawAggBuckets.indices) {
                assertEquals("Avg Fare amounts are not the same", rawAggBuckets[i]["fareAmount"], transformAggBuckets[i]["fareAmount"])
                assertEquals("Max pickup date times are not the same", rawAggBuckets[i][pickupDateTime]!!["value"], transformAggBuckets[i][pickupDateTime]!!["value"])
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test transform term on date field and aggregation on date field`() {
        val sourceIdxTestName = "source_idx_test_16"
        val targetIdxTestName = "target_idx_test_16"

        val pickupDateTime = "tpep_pickup_datetime"
        val pickupDateTimeTerm = pickupDateTime.plus("_term")
        val fareAmount = "fare_amount"

        validateSourceIndex(sourceIdxTestName)

        val avgFareAmountAgg = AggregationBuilders.avg(fareAmount).field(fareAmount)
        val countDateAggBuilder = AggregationBuilders.count(pickupDateTime).field(pickupDateTime)

        val transform = Transform(
            id = "id_16",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform doc values must be the same",
            metadataId = null,
            sourceIndex = sourceIdxTestName,
            targetIndex = targetIdxTestName,
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = pickupDateTime, targetField = pickupDateTimeTerm),
            ),
            aggregations = AggregatorFactories.builder().addAggregator(avgFareAmountAgg).addAggregator(countDateAggBuilder),
        ).let { createTransform(it, it.id) }
        updateTransformStartTime(transform)

        waitFor {
            assertTrue("Target transform index was not created", indexExists(transform.targetIndex))
        }

        waitFor(Instant.ofEpochSecond(180)) {
            val transformJob = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", transformJob.metadataId)
            val transformMetadata = getTransformMetadata(transformJob.metadataId!!)
            assertEquals("Transform is not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
        }

        val sourceIndexMapping = client().makeRequest("GET", "/$sourceIdxTestName/_mapping")
        val sourceIndexParserMap = createParser(XContentType.JSON.xContent(), sourceIndexMapping.entity.content).map() as Map<String, Map<String, Any>>
        val targetIndexMapping = client().makeRequest("GET", "/$targetIdxTestName/_mapping")
        val targetIndexParserMap = createParser(XContentType.JSON.xContent(), targetIndexMapping.entity.content).map() as Map<String, Map<String, Any>>

        val sourceProperties = ((sourceIndexParserMap[sourceIdxTestName]?.get("mappings") as Map<String, Any>)["properties"] as Map<String, Any>)
        val targetProperties = ((targetIndexParserMap[targetIdxTestName]?.get("mappings") as Map<String, Any>)["properties"] as Map<String, Any>)

        val sourcePickupDate = (sourceProperties [pickupDateTime] as Map<String, Any>)["type"]
        val targetPickupDateTerm = (targetProperties [pickupDateTimeTerm] as Map<String, Any>)["type"]

        assertEquals("date", targetPickupDateTerm)
        assertEquals(sourcePickupDate, targetPickupDateTerm)

        val targetPickupDate = (targetProperties [pickupDateTime] as Map<String, Any>)["type"]

        assertEquals("date", targetPickupDate)
        assertEquals(sourcePickupDate, targetPickupDate)

        val sourceRequest = """
            {
                "size": 0,
                "aggs": {
                    "$pickupDateTimeTerm": {
                        "terms": {
                            "field": "$pickupDateTime", "order": { "_key": "asc" }
                        },
                        "aggs": {
                           "$fareAmount": { "avg": { "field": "$fareAmount" } },
                           "$pickupDateTime": {"value_count": {"field": "$pickupDateTime"}}
                        }
                    }
                }
            }
            """

        val targetRequest = """
            {
                "size": 0,
                "aggs": {
                    "$pickupDateTimeTerm": {
                        "terms": {
                            "field": "$pickupDateTimeTerm", "order": { "_key": "asc" }
                        },
                        "aggs": {
                           "$fareAmount": { "avg": { "field": "$fareAmount" } },
                           "$pickupDateTime": {"value_count": {"field": "$pickupDateTime"}}
                        }
                    }
                }
            }
            """

        var rawRes = client().makeRequest(RestRequest.Method.POST.name, "/$sourceIdxTestName/_search", emptyMap(), StringEntity(sourceRequest, ContentType.APPLICATION_JSON))
        assertTrue(rawRes.restStatus() == RestStatus.OK)

        var transformRes = client().makeRequest(RestRequest.Method.POST.name, "/$targetIdxTestName/_search", emptyMap(), StringEntity(targetRequest, ContentType.APPLICATION_JSON))
        assertTrue(transformRes.restStatus() == RestStatus.OK)

        val rawAggBuckets = (rawRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)[pickupDateTimeTerm]!!["buckets"]!!
        val transformAggBuckets = (transformRes.asMap()["aggregations"] as Map<String, Map<String, List<Map<String, Map<String, Any>>>>>)[pickupDateTimeTerm]!!["buckets"]!!

        assertEquals("Different bucket sizes", rawAggBuckets.size, transformAggBuckets.size)

        for (i in rawAggBuckets.indices) {
            assertEquals("Term pickup date bucket keys are not the same", rawAggBuckets[i]["key"], transformAggBuckets[i]["key"])
            assertEquals("Avg fare amounts are not the same", rawAggBuckets[i]["fareAmount"], transformAggBuckets[i]["fareAmount"])
            assertEquals("Count pickup dates are not the same", rawAggBuckets[i][pickupDateTime]!!["value"], transformAggBuckets[i][pickupDateTime]!!["value"])
        }
    }

    fun `test transform with failure during indexing`() {
        validateSourceIndex("transform-source-index")

        // Indexing failure because target index is strictly mapped
        createIndex("transform-target-strict-index", Settings.EMPTY, getStrictMappings())
        waitFor {
            assertTrue("Strict target index not created", indexExists("transform-target-strict-index"))
        }
        val transform = Transform(
            id = "id_5",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-strict-index",
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
            ),
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not failed", TransformMetadata.Status.FAILED, transformMetadata.status)
            transformMetadata
        }

        assertTrue("Expected failure message to be present", !metadata.failureReason.isNullOrBlank())
    }

    fun `test transform with invalid aggregation triggering search failure`() {
        validateSourceIndex("transform-source-index")

        val aggregatorFactories = AggregatorFactories.builder()
        aggregatorFactories.addAggregator(
            ScriptedMetricAggregationBuilder("average_revenue_per_passenger_per_trip")
                .initScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "state.count = 0; state.sum = 0;", emptyMap()))
                .mapScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "state.sum += doc[\"random_field\"].value; state.count += doc[\"passenger_count\"].value",
                        emptyMap(),
                    ),
                )
                .combineScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "def d = new long[2]; d[0] = state.sum; d[1] = state.count; return d",
                        emptyMap(),
                    ),
                )
                .reduceScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "double sum = 0; double count = 0; for (a in states) { sum += a[0]; count += a[1]; } return sum/count",
                        emptyMap(),
                    ),
                ),
        )

        val transform = Transform(
            id = "id_6",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-source-index",
            targetIndex = "transform-target-index",
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
                Histogram(sourceField = "passenger_count", targetField = "count", interval = 2.0),
                DateHistogram(sourceField = "tpep_pickup_datetime", targetField = "date", fixedInterval = "1d"),
            ),
            aggregations = aggregatorFactories,
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not failed", TransformMetadata.Status.FAILED, transformMetadata.status)
            transformMetadata
        }

        assertTrue("Expected failure message to be present", !metadata.failureReason.isNullOrBlank())
    }

    fun `test transform with invalid pipeline aggregation triggering search failure`() {
        assertFailsWith(IllegalArgumentException::class, "Bucket-script aggregation must fail!") {
            validateSourceIndex("transform-source-index")

            val aggregatorFactories = AggregatorFactories.builder()
            aggregatorFactories.addPipelineAggregator(
                BucketScriptPipelineAggregationBuilder(
                    "test_pipeline_aggregation",
                    Script("1"),
                ),
            )

            val transform = Transform(
                id = "id_17",
                schemaVersion = 1L,
                enabled = true,
                enabledAt = Instant.now(),
                updatedAt = Instant.now(),
                jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                description = "test transform",
                metadataId = null,
                sourceIndex = "transform-source-index",
                targetIndex = "transform-target-index",
                roles = emptyList(),
                pageSize = 1,
                groups = listOf(
                    Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
                    Histogram(sourceField = "passenger_count", targetField = "count", interval = 2.0),
                    DateHistogram(sourceField = "tpep_pickup_datetime", targetField = "date", fixedInterval = "1d"),
                ),
                aggregations = aggregatorFactories,
            ).let { createTransform(it, it.id) }
            updateTransformStartTime(transform)
        }
    }

    fun `test transform with data stream`() {
        // Create a data stream.
        val dataStreamName = "transform-data-stream"
        client().makeRequest(
            "PUT",
            "/_index_template/transform-data-stream-template",
            StringEntity(
                """
                {
                    "data_stream": {"timestamp_field": {"name": "tpep_pickup_datetime"}},
                    "index_patterns": ["$dataStreamName"],
                    "template": {
                        "mappings": {
                            "properties":{"DOLocationID":{"type":"integer"},"RatecodeID":{"type":"integer"},"fare_amount":{"type":"float"},"tpep_dropoff_datetime":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"congestion_surcharge":{"type":"float"},"VendorID":{"type":"integer"},"passenger_count":{"type":"integer"},"tolls_amount":{"type":"float"},"improvement_surcharge":{"type":"float"},"trip_distance":{"type":"float"},"store_and_fwd_flag":{"type":"keyword"},"payment_type":{"type":"integer"},"total_amount":{"type":"float"},"extra":{"type":"float"},"tip_amount":{"type":"float"},"mta_tax":{"type":"float"},"tpep_pickup_datetime":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"PULocationID":{"type":"integer"}}
                        }
                    }
                }
                """.trimIndent(),
                ContentType.APPLICATION_JSON,
            ),
        )
        client().makeRequest("PUT", "/_data_stream/$dataStreamName")

        // Insert the sample data across multiple backing indices of the data stream.
        insertSampleBulkData(dataStreamName, javaClass.classLoader.getResource("data/nyc_5000.ndjson").readText())
        client().makeRequest("POST", "/$dataStreamName/_rollover")
        insertSampleBulkData(dataStreamName, javaClass.classLoader.getResource("data/nyc_5000.ndjson").readText())
        client().makeRequest("POST", "/$dataStreamName/_rollover")

        // Create the transform job.
        val transform = Transform(
            id = "id_7",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = dataStreamName,
            targetIndex = "transform-target-index",
            roles = emptyList(),
            pageSize = 100,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
            ),
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor { assertTrue("Target transform index was not created", indexExists(transform.targetIndex)) }

        val metadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform has not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
            transformMetadata
        }

        assertEquals("More than expected pages processed", 2L, metadata.stats.pagesProcessed)
        assertEquals("More than expected documents indexed", 2L, metadata.stats.documentsIndexed)
        assertEquals("More than expected documents processed", 10000L, metadata.stats.documentsProcessed)
        // In some cases it seems that these times are less than 1ms - which causes fails on ubuntu instances (at least that was detected)
        assertTrue("Doesn't capture indexed time", metadata.stats.indexTimeInMillis >= 0)
        assertTrue("Didn't capture search time", metadata.stats.searchTimeInMillis >= 0)
    }

    fun `test no-op execution when no buckets have been modified`() {
        validateSourceIndex("transform-no-op-source-index")

        val transform = Transform(
            id = "id_8",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "transform-no-op-source-index",
            targetIndex = "transform-no-op-target-index",
            roles = emptyList(),
            pageSize = 100,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
                Histogram(sourceField = "trip_distance", targetField = "distance", interval = 0.1),
            ),
            continuous = true,
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor { assertTrue("Target transform index was not created", indexExists(transform.targetIndex)) }

        val firstIterationMetadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform did not complete iteration or had incorrect number of documents processed", 5000, transformMetadata.stats.documentsProcessed)
            assertEquals("Transform did not complete iteration", null, transformMetadata.afterKey)
            assertNotNull("Continuous stats were not updated", transformMetadata.continuousStats)
            assertNotNull("Continuous stats were set, but lastTimestamp was not", transformMetadata.continuousStats!!.lastTimestamp)
            transformMetadata
        }

        assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, firstIterationMetadata.status)
        assertEquals("Not the expected pages processed", 3L, firstIterationMetadata.stats.pagesProcessed)
        assertEquals("Not the expected documents indexed", 198L, firstIterationMetadata.stats.documentsIndexed)
        assertEquals("Not the expected documents processed", 5000L, firstIterationMetadata.stats.documentsProcessed)
        assertTrue("Didn't capture indexed time", firstIterationMetadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", firstIterationMetadata.stats.searchTimeInMillis > 0)

        updateTransformStartTime(transform)

        val secondIterationMetadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertTrue("Transform did not complete iteration or update timestamp", transformMetadata.continuousStats!!.lastTimestamp!! > firstIterationMetadata.continuousStats!!.lastTimestamp)
            transformMetadata
        }

        assertEquals("Transform did not have null afterKey after iteration", null, secondIterationMetadata.afterKey)
        assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, firstIterationMetadata.status)
        assertEquals("Not the expected pages processed", firstIterationMetadata.stats.pagesProcessed, secondIterationMetadata.stats.pagesProcessed)
        assertEquals("Not the expected documents indexed", firstIterationMetadata.stats.documentsIndexed, secondIterationMetadata.stats.documentsIndexed)
        assertEquals("Not the expected documents processed", firstIterationMetadata.stats.documentsProcessed, secondIterationMetadata.stats.documentsProcessed)
        assertEquals("Not the expected index time", firstIterationMetadata.stats.indexTimeInMillis, secondIterationMetadata.stats.indexTimeInMillis)
        assertEquals("Not the expected search time", firstIterationMetadata.stats.searchTimeInMillis, secondIterationMetadata.stats.searchTimeInMillis)

        disableTransform(transform.id)
    }

    fun `test continuous transform picks up new documents`() {
        validateSourceIndex("continuous-transform-source-index")

        val aggregatorFactories = AggregatorFactories.builder()
        aggregatorFactories.addAggregator(AggregationBuilders.sum("revenue").field("total_amount"))

        val transform = Transform(
            id = "id_9",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "continuous-transform-source-index",
            targetIndex = "continuous-transform-target-index",
            roles = emptyList(),
            pageSize = 100,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
            ),
            continuous = true,
            aggregations = aggregatorFactories,
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor { assertTrue("Target transform index was not created", indexExists(transform.targetIndex)) }

        val firstIterationMetadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform did not complete iteration or had incorrect number of documents processed", 5000, transformMetadata.stats.documentsProcessed)
            assertEquals("Transform did not complete iteration", null, transformMetadata.afterKey)
            transformMetadata
        }

        assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, firstIterationMetadata.status)
        assertEquals("Not the expected pages processed", 2L, firstIterationMetadata.stats.pagesProcessed)
        assertEquals("Not the expected documents indexed", 2L, firstIterationMetadata.stats.documentsIndexed)
        assertEquals("Not the expected documents processed", 5000L, firstIterationMetadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", firstIterationMetadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", firstIterationMetadata.stats.searchTimeInMillis > 0)

        var hits = waitFor {
            val response = client().makeRequest(
                "GET", "continuous-transform-target-index/_search",
                StringEntity("{}", ContentType.APPLICATION_JSON),
            )
            assertEquals("Request failed", RestStatus.OK, response.restStatus())
            val responseHits = response.asMap().getValue("hits") as Map<*, *>
            val totalDocs = (responseHits["hits"] as ArrayList<*>).fold(0) { sum, bucket ->
                val docCount = ((bucket as Map<*, *>)["_source"] as Map<*, *>)["_doc_count"] as Int
                sum + docCount
            }
            assertEquals("Not all documents included in the transform target index", 5000, totalDocs)

            responseHits["hits"] as ArrayList<*>
        }
        hits.forEach {
            val bucket = ((it as Map<*, *>)["_source"] as Map<*, *>)
            if (bucket["flag"] == "N") {
                assertEquals("Transform sum not calculated correctly", 76547.9, bucket["revenue"] as Double, 0.1)
            } else {
                assertEquals("Transform sum not calculated correctly", 359.8, bucket["revenue"] as Double, 0.1)
            }
        }

        // Add the same 5000 documents again, and start another execution
        insertSampleBulkData(transform.sourceIndex, javaClass.classLoader.getResource("data/nyc_5000.ndjson").readText())

        waitFor {
            val documentsBehind = getTransformDocumentsBehind(transform.id)[transform.sourceIndex]
            assertEquals("Documents behind not calculated correctly", 5000, documentsBehind)
        }

        updateTransformStartTime(transform)

        val secondIterationMetadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            // As the new documents all fall into the same buckets as the last, all of the documents are processed again
            assertEquals("Transform did not complete iteration or had incorrect number of documents processed", 15000, transformMetadata.stats.documentsProcessed)
            assertEquals("Transform did not have null afterKey after iteration", null, transformMetadata.afterKey)
            transformMetadata
        }

        assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, secondIterationMetadata.status)
        assertEquals("More than expected pages processed", 4L, secondIterationMetadata.stats.pagesProcessed)
        assertEquals("More than expected documents indexed", 4L, secondIterationMetadata.stats.documentsIndexed)
        assertEquals("Not the expected documents processed", 15000L, secondIterationMetadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", secondIterationMetadata.stats.indexTimeInMillis > firstIterationMetadata.stats.indexTimeInMillis)
        assertTrue("Didn't capture search time", secondIterationMetadata.stats.searchTimeInMillis > firstIterationMetadata.stats.searchTimeInMillis)

        hits = waitFor {
            val response = client().makeRequest(
                "GET", "continuous-transform-target-index/_search",
                StringEntity("{}", ContentType.APPLICATION_JSON),
            )
            assertEquals("Request failed", RestStatus.OK, response.restStatus())
            val responseHits = response.asMap().getValue("hits") as Map<*, *>
            val totalDocs = (responseHits["hits"] as ArrayList<*>).fold(0) { sum, bucket ->
                val docCount = ((bucket as Map<*, *>)["_source"] as Map<*, *>)["_doc_count"] as Int
                sum + docCount
            }
            assertEquals("Not all documents included in the transform target index", 10000, totalDocs)

            responseHits["hits"] as ArrayList<*>
        }
        hits.forEach {
            val bucket = ((it as Map<*, *>)["_source"] as Map<*, *>)
            if (bucket["flag"] == "N") {
                assertEquals("Transform sum not calculated correctly", 153095.9, bucket["revenue"] as Double, 0.1)
            } else {
                assertEquals("Transform sum not calculated correctly", 719.7, bucket["revenue"] as Double, 0.1)
            }
        }
        disableTransform(transform.id)
    }

    fun `test continuous transform only transforms modified buckets`() {
        val sourceIndex = "modified-bucket-source-index"
        createIndex(sourceIndex, Settings.EMPTY, """"properties":{"iterating_id":{"type":"integer"},"twice_id":{"type":"integer"}}""")
        for (i in 0..47) {
            val jsonString = "{\"iterating_id\": \"$i\",\"twice_id\": \"${i * 2}\"}"
            insertSampleData(sourceIndex, 1, jsonString = jsonString)
        }

        val aggregatorFactories = AggregatorFactories.builder()
        aggregatorFactories.addAggregator(AggregationBuilders.sum("twice_id_sum").field("twice_id"))

        val transform = Transform(
            id = "id_10",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = sourceIndex,
            targetIndex = "modified-bucket-target-index",
            roles = emptyList(),
            pageSize = 100,
            groups = listOf(
                Histogram(sourceField = "iterating_id", targetField = "id_group", interval = 5.0),
            ),
            continuous = true,
            aggregations = aggregatorFactories,
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor { assertTrue("Target transform index was not created", indexExists(transform.targetIndex)) }

        val firstIterationMetadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform did not complete iteration or had incorrect number of documents processed", 48, transformMetadata.stats.documentsProcessed)
            assertEquals("Transform did not complete iteration", null, transformMetadata.afterKey)
            transformMetadata
        }

        assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, firstIterationMetadata.status)
        assertEquals("Not the expected pages processed", 2L, firstIterationMetadata.stats.pagesProcessed)
        assertEquals("Not the expected documents indexed", 10L, firstIterationMetadata.stats.documentsIndexed)
        assertEquals("Not the expected documents processed", 48L, firstIterationMetadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", firstIterationMetadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", firstIterationMetadata.stats.searchTimeInMillis > 0)

        // Get all of the buckets
        var hits = waitFor {
            val response = client().makeRequest(
                "GET", "${transform.targetIndex}/_search",
                StringEntity("{\"size\": 25}", ContentType.APPLICATION_JSON),
            )
            assertEquals("Request failed", RestStatus.OK, response.restStatus())
            val responseHits = response.asMap().getValue("hits") as Map<*, *>
            val totalDocs = (responseHits["hits"] as ArrayList<*>).fold(0) { sum, bucket ->
                val docCount = ((bucket as Map<*, *>)["_source"] as Map<*, *>)["_doc_count"] as Int
                sum + docCount
            }
            assertEquals("Not all documents included in the transform target index", 48, totalDocs)

            responseHits["hits"] as ArrayList<*>
        }

        // Validate the buckets include the correct information
        hits.forEach {
            val bucket = ((it as Map<*, *>)["_source"] as Map<*, *>)
            val idGroup = (bucket["id_group"] as Double).toInt()
            if (idGroup != 45) {
                val expectedSum = ((idGroup * 2)..((idGroup * 2) + 8) step 2).sum()
                assertEquals("ID sum not calculated correctly", expectedSum, (bucket["twice_id_sum"] as Double).toInt())
            } else {
                // The last bucket will only be partially filled
                assertEquals("ID sum not calculated correctly", 276, (bucket["twice_id_sum"] as Double).toInt())
            }
        }
        // Add more data
        for (i in 48..99) {
            val jsonString = "{\"iterating_id\": \"$i\",\"twice_id\": \"${i * 2}\"}"
            insertSampleData(sourceIndex, 1, jsonString = jsonString)
        }

        waitFor {
            val documentsBehind = getTransformDocumentsBehind(transform.id)[transform.sourceIndex]
            assertEquals("Documents behind not calculated correctly", 52, documentsBehind)
        }

        updateTransformStartTime(transform)

        val secondIterationMetadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            // As the ids 45-47 will be processed a second time when the bucket is recalculated, this number is greater than 100
            assertEquals("Transform did not complete iteration or had incorrect number of documents processed", 103L, transformMetadata.stats.documentsProcessed)
            assertEquals("Transform did not have null afterKey after iteration", null, transformMetadata.afterKey)
            transformMetadata
        }

        assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, secondIterationMetadata.status)
        assertEquals("More than expected pages processed", 4L, secondIterationMetadata.stats.pagesProcessed)
        assertEquals("More than expected documents indexed", 21L, secondIterationMetadata.stats.documentsIndexed)
        assertEquals("Not the expected documents processed", 103L, secondIterationMetadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", secondIterationMetadata.stats.indexTimeInMillis > firstIterationMetadata.stats.indexTimeInMillis)
        assertTrue("Didn't capture search time", secondIterationMetadata.stats.searchTimeInMillis > firstIterationMetadata.stats.searchTimeInMillis)

        disableTransform(transform.id)

        hits = waitFor {
            val response = client().makeRequest(
                "GET", "${transform.targetIndex}/_search",
                StringEntity("{\"size\": 25}", ContentType.APPLICATION_JSON),
            )
            assertEquals("Request failed", RestStatus.OK, response.restStatus())
            val responseHits = response.asMap().getValue("hits") as Map<*, *>
            val totalDocs = (responseHits["hits"] as ArrayList<*>).fold(0) { sum, bucket ->
                val docCount = ((bucket as Map<*, *>)["_source"] as Map<*, *>)["_doc_count"] as Int
                sum + docCount
            }
            assertEquals("Not all documents included in the transform target index", 100, totalDocs)

            responseHits["hits"] as ArrayList<*>
        }

        hits.forEach {
            val bucket = ((it as Map<*, *>)["_source"] as Map<*, *>)
            val idGroup = (bucket["id_group"] as Double).toInt()
            val expectedSum = ((idGroup * 2)..((idGroup * 2) + 8) step 2).sum()
            assertEquals("ID sum not calculated correctly", expectedSum, (bucket["twice_id_sum"] as Double).toInt())
        }
    }

    fun `test continuous transform with wildcard indices`() {
        validateSourceIndex("wildcard-source-1")
        validateSourceIndex("wildcard-source-2")
        validateSourceIndex("wildcard-source-3")

        val transform = Transform(
            id = "id_11",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "wildcard-s*e-*",
            targetIndex = "wildcard-target-index",
            roles = emptyList(),
            pageSize = 100,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
            ),
            continuous = true,
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor { assertTrue("Target transform index was not created", indexExists(transform.targetIndex)) }

        val firstIterationMetadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform did not complete iteration or had incorrect number of documents processed", 15000, transformMetadata.stats.documentsProcessed)
            assertEquals("Transform did not complete iteration", null, transformMetadata.afterKey)
            assertNotNull("Continuous stats were not updated", transformMetadata.continuousStats)
            assertNotNull("Continuous stats were set, but lastTimestamp was not", transformMetadata.continuousStats!!.lastTimestamp)
            assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, transformMetadata.status)
            assertEquals("Not the expected pages processed", 6L, transformMetadata.stats.pagesProcessed)
            assertEquals("Not the expected documents indexed", 2L, transformMetadata.stats.documentsIndexed)
            assertEquals("Not the expected documents processed", 15000L, transformMetadata.stats.documentsProcessed)
            assertTrue("Doesn't capture indexed time", transformMetadata.stats.indexTimeInMillis > 0)
            assertTrue("Didn't capture search time", transformMetadata.stats.searchTimeInMillis > 0)
            transformMetadata
        }

        waitFor {
            val documentsBehind = getTransformDocumentsBehind(transform.id)
            assertNotNull(documentsBehind)
            assertEquals("Not the expected documents behind", 0, documentsBehind.values.sumOf { it as Int })
        }

        // Start the continuous transform again, and make sure it was a no-op
        updateTransformStartTime(transform)

        Thread.sleep(5000)

        waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform did not complete iteration or had incorrect number of documents processed", 15000, transformMetadata.stats.documentsProcessed)
            assertEquals("Transform did not have null afterKey after iteration", null, transformMetadata.afterKey)
            assertTrue("Timestamp was not updated", transformMetadata.continuousStats!!.lastTimestamp!!.isAfter(firstIterationMetadata.continuousStats!!.lastTimestamp))
            assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, transformMetadata.status)
            assertEquals("More than expected pages processed", 6, transformMetadata.stats.pagesProcessed)
            assertEquals("More than expected documents indexed", 2L, transformMetadata.stats.documentsIndexed)
            assertEquals("Not the expected documents processed", 15000L, transformMetadata.stats.documentsProcessed)
            assertEquals("Not the expected indexed time", transformMetadata.stats.indexTimeInMillis, firstIterationMetadata.stats.indexTimeInMillis)
            assertEquals("Not the expected search time", transformMetadata.stats.searchTimeInMillis, firstIterationMetadata.stats.searchTimeInMillis)
        }

        disableTransform(transform.id)
    }

    fun `test continuous transforms with null buckets`() {
        val sourceIndex = "null-bucket-source-index"
        createIndex(sourceIndex, Settings.EMPTY, """"properties":{"iterating_id":{"type":"integer"},"term_id":{"type":"keyword"},"twice_id":{"type":"integer"}}""")
        for (i in 0..12) {
            val jsonString = "{\"iterating_id\": \"$i\",\"term_id\": \"${i % 5}\",\"twice_id\": \"${i * 2}\"}"
            insertSampleData(sourceIndex, 1, jsonString = jsonString)
            val idNullJsonString = "{\"iterating_id\": null,\"term_id\": \"${i % 5}\",\"twice_id\": \"${i * 2}\"}"
            insertSampleData(sourceIndex, 1, jsonString = idNullJsonString)
            val termNullJsonString = "{\"iterating_id\": \"$i\",\"term_id\": null,\"twice_id\": \"${i * 2}\"}"
            insertSampleData(sourceIndex, 1, jsonString = termNullJsonString)
            val bothNullJsonString = "{\"iterating_id\": null,\"term_id\": null,\"twice_id\": \"${i * 2}\"}"
            insertSampleData(sourceIndex, 1, jsonString = bothNullJsonString)
        }

        val aggregatorFactories = AggregatorFactories.builder()
        aggregatorFactories.addAggregator(AggregationBuilders.sum("twice_id_sum").field("twice_id"))

        val transform = Transform(
            id = "id_12",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = sourceIndex,
            targetIndex = "null-bucket-target-index",
            roles = emptyList(),
            pageSize = 100,
            groups = listOf(
                Histogram(sourceField = "iterating_id", targetField = "id_group", interval = 5.0),
                Terms(sourceField = "term_id", targetField = "id_term"),
            ),
            continuous = true,
            aggregations = aggregatorFactories,
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor { assertTrue("Target transform index was not created", indexExists(transform.targetIndex)) }

        val firstIterationMetadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform did not complete iteration or had incorrect number of documents processed", 52, transformMetadata.stats.documentsProcessed)
            assertEquals("Transform did not complete iteration", null, transformMetadata.afterKey)
            transformMetadata
        }

        assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, firstIterationMetadata.status)
        assertEquals("Not the expected pages processed", 2L, firstIterationMetadata.stats.pagesProcessed)
        assertEquals("Not the expected documents indexed", 22L, firstIterationMetadata.stats.documentsIndexed)
        assertEquals("Not the expected documents processed", 52L, firstIterationMetadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", firstIterationMetadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", firstIterationMetadata.stats.searchTimeInMillis > 0)

        // Get all the buckets
        var hits = waitFor {
            val response = client().makeRequest(
                "GET", "${transform.targetIndex}/_search",
                StringEntity("{\"size\": 25}", ContentType.APPLICATION_JSON),
            )
            assertEquals("Request failed", RestStatus.OK, response.restStatus())
            val responseHits = response.asMap().getValue("hits") as Map<*, *>
            val totalDocs = (responseHits["hits"] as ArrayList<*>).fold(0) { sum, bucket ->
                val docCount = ((bucket as Map<*, *>)["_source"] as Map<*, *>)["_doc_count"] as Int
                sum + docCount
            }
            assertEquals("Not all documents included in the transform target index", 52, totalDocs)

            responseHits["hits"] as ArrayList<*>
        }

        // Validate the buckets include the correct information
        hits.forEach {
            val bucket = ((it as Map<*, *>)["_source"] as Map<*, *>)
            val idGroup = (bucket["id_group"] as Double?)?.toInt()
            val idTerm = (bucket["id_term"] as String?)?.toInt()
            if (idGroup == null) {
                if (idTerm == null) {
                    val expectedSum = (0..(24) step 2).sum()
                    assertEquals("ID sum not calculated correctly", expectedSum, (bucket["twice_id_sum"] as Double).toInt())
                } else {
                    val expectedSum = ((idTerm * 2)..(24) step 10).sum()
                    assertEquals("ID sum not calculated correctly", expectedSum, (bucket["twice_id_sum"] as Double).toInt())
                }
            } else if (idTerm == null) {
                // use the min to get the correct sum for the half full top bucket
                val expectedSum = ((idGroup * 2)..(min(idGroup * 2 + 8, 24)) step 2).sum()
                assertEquals("ID sum not calculated correctly", expectedSum, (bucket["twice_id_sum"] as Double).toInt())
            } else {
                val expectedSum = idGroup * 2 + idTerm * 2
                assertEquals("ID sum not calculated correctly", expectedSum, (bucket["twice_id_sum"] as Double).toInt())
            }
        }

        // Add more data, don't add any to the (null, null) bucket to check that it won't be updated without new data
        for (i in 13..24) {
            val jsonString = "{\"iterating_id\": \"$i\",\"term_id\": \"${i % 5}\",\"twice_id\": \"${i * 2}\"}"
            insertSampleData(sourceIndex, 1, jsonString = jsonString)
            val idNullJsonString = "{\"iterating_id\": null,\"term_id\": \"${i % 5}\",\"twice_id\": \"${i * 2}\"}"
            insertSampleData(sourceIndex, 1, jsonString = idNullJsonString)
            val termNullJsonString = "{\"iterating_id\": \"$i\",\"term_id\": null,\"twice_id\": \"${i * 2}\"}"
            insertSampleData(sourceIndex, 1, jsonString = termNullJsonString)
        }

        waitFor {
            val documentsBehind = getTransformDocumentsBehind(transform.id)[transform.sourceIndex]
            assertEquals("Documents behind not calculated correctly", 36, documentsBehind)
        }

        updateTransformStartTime(transform)

        val secondIterationMetadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform did not complete iteration or had incorrect number of documents processed", 104L, transformMetadata.stats.documentsProcessed)
            assertEquals("Transform did not have null afterKey after iteration", null, transformMetadata.afterKey)
            transformMetadata
        }

        assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, secondIterationMetadata.status)
        assertEquals("More than expected pages processed", 4L, secondIterationMetadata.stats.pagesProcessed)
        assertEquals("More than expected documents indexed", 42L, secondIterationMetadata.stats.documentsIndexed)
        assertEquals("Not the expected documents processed", 104L, secondIterationMetadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", secondIterationMetadata.stats.indexTimeInMillis > firstIterationMetadata.stats.indexTimeInMillis)
        assertTrue("Didn't capture search time", secondIterationMetadata.stats.searchTimeInMillis > firstIterationMetadata.stats.searchTimeInMillis)

        disableTransform(transform.id)

        hits = waitFor {
            val response = client().makeRequest(
                "GET", "${transform.targetIndex}/_search",
                StringEntity("{\"size\": 40}", ContentType.APPLICATION_JSON),
            )
            assertEquals("Request failed", RestStatus.OK, response.restStatus())
            val responseHits = response.asMap().getValue("hits") as Map<*, *>
            val totalDocs = (responseHits["hits"] as ArrayList<*>).fold(0) { sum, bucket ->
                val docCount = ((bucket as Map<*, *>)["_source"] as Map<*, *>)["_doc_count"] as Int
                sum + docCount
            }
            assertEquals("Not all documents included in the transform target index", 88, totalDocs)

            responseHits["hits"] as ArrayList<*>
        }

        // Validate the buckets include the correct information
        hits.forEach {
            val bucket = ((it as Map<*, *>)["_source"] as Map<*, *>)
            val idGroup = (bucket["id_group"] as Double?)?.toInt()
            val idTerm = (bucket["id_term"] as String?)?.toInt()
            if (idGroup == null) {
                if (idTerm == null) {
                    val expectedSum = (0..(24) step 2).sum()
                    assertEquals("ID sum not calculated correctly", expectedSum, (bucket["twice_id_sum"] as Double).toInt())
                } else {
                    val expectedSum = ((idTerm * 2)..(48) step 10).sum()
                    assertEquals("ID sum not calculated correctly", expectedSum, (bucket["twice_id_sum"] as Double).toInt())
                }
            } else if (idTerm == null) {
                // use the min to get the correct sum for the half full top bucket
                val expectedSum = ((idGroup * 2)..(idGroup * 2 + 8) step 2).sum()
                assertEquals("ID sum not calculated correctly", expectedSum, (bucket["twice_id_sum"] as Double).toInt())
            } else {
                val expectedSum = idGroup * 2 + idTerm * 2
                assertEquals("ID sum not calculated correctly", expectedSum, (bucket["twice_id_sum"] as Double).toInt())
            }
        }
    }

    fun `test continuous transform with a lot of buckets`() {
        // Create index with high cardinality fields
        val sourceIndex = "index_with_lots_of_buckets"

        val requestBody: StringBuilder = StringBuilder(100000)
        for (i in 1..2000) {
            val docPayload: String = """
            {
              "id1": "$i",
              "id2": "${i + 1}"
            }
            """.trimIndent().replace(Regex("[\n\r\\s]"), "")

            requestBody.append("{\"create\":{}}\n").append(docPayload).append('\n')
        }

        createIndexAndBulkInsert(sourceIndex, Settings.EMPTY, null, null, requestBody.toString())
        // Source index will have total of 2000 buckets
        val transform = Transform(
            id = "transform_index_with_lots_of_buckets",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = "index_with_lots_of_buckets",
            targetIndex = "index_with_lots_of_buckets_transformed",
            roles = emptyList(),
            pageSize = 1000,
            groups = listOf(
                Terms(sourceField = "id1.keyword", targetField = "id1"),
                Terms(sourceField = "id2.keyword", targetField = "id2"),
            ),
            continuous = true,
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor { assertTrue("Target transform index was not created", indexExists(transform.targetIndex)) }

        val firstIterationMetadata = waitFor {
            val job = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", job.metadataId)
            val transformMetadata = getTransformMetadata(job.metadataId!!)
            assertEquals("Transform did not complete iteration or had incorrect number of documents processed", 2000, transformMetadata.stats.documentsProcessed)
            assertEquals("Transform did not complete iteration", null, transformMetadata.afterKey)
            assertNotNull("Continuous stats were not updated", transformMetadata.continuousStats)
            assertNotNull("Continuous stats were set, but lastTimestamp was not", transformMetadata.continuousStats!!.lastTimestamp)
            transformMetadata
        }

        assertEquals("Not the expected transform status", TransformMetadata.Status.STARTED, firstIterationMetadata.status)
        assertEquals("Not the expected pages processed", 7, firstIterationMetadata.stats.pagesProcessed)
        assertEquals("Not the expected documents indexed", 2000L, firstIterationMetadata.stats.documentsIndexed)
        assertEquals("Not the expected documents processed", 2000L, firstIterationMetadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", firstIterationMetadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", firstIterationMetadata.stats.searchTimeInMillis > 0)

        disableTransform(transform.id)
    }

    private fun getStrictMappings(): String {
        return """
            "dynamic": "strict",
            "properties": {
                "some-column": {
                    "type": "keyword"
                }
            }
        """.trimIndent()
    }

    private fun validateSourceIndex(indexName: String) {
        if (!indexExists(indexName)) {
            generateNYCTaxiData(indexName)
            assertIndexExists(indexName)
        }
    }

    private fun createIndexAndBulkInsert(name: String, settings: Settings?, mapping: String?, aliases: String?, bulkData: String) {
        if (settings != null || mapping != null || aliases != null) {
            createIndex(name, settings, mapping, aliases)
        }

        val request = Request("POST", "/$name/_bulk/?refresh=true")
        request.setJsonEntity(bulkData)
        request.options = RequestOptions.DEFAULT.toBuilder().addHeader("content-type", "application/x-ndjson").build()
        var res = client().performRequest(request)
        assertEquals(RestStatus.OK, res.restStatus())

        val refreshRequest = Request("POST", "/$name/_refresh")
        res = client().performRequest(refreshRequest)
        assertEquals(RestStatus.OK, res.restStatus())
    }
}
