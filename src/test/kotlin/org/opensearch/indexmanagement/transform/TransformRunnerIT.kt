/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.common.settings.Settings
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Histogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder
import java.time.Instant
import java.time.temporal.ChronoUnit

class TransformRunnerIT : TransformRestTestCase() {

    fun `test transform`() {
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

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
            pageSize = 100,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            )
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
        assertEquals("More than expected documents processed", 5000L, metadata.stats.documentsProcessed)
        assertTrue("Doesn't capture indexed time", metadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", metadata.stats.searchTimeInMillis > 0)
    }

    fun `test transform with data filter`() {
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

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
            pageSize = 100,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            ),
            dataSelectionQuery = TermQueryBuilder("store_and_fwd_flag", "N")
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
        assertTrue("Doesn't capture indexed time", metadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", metadata.stats.searchTimeInMillis > 0)
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
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

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
                        emptyMap()
                    )
                )
                .combineScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "def d = new long[2]; d[0] = state.sum; d[1] = state.count; return d",
                        emptyMap()
                    )
                )
                .reduceScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "double sum = 0; double count = 0; for (a in states) { sum += a[0]; count += a[1]; } return sum/count",
                        emptyMap()
                    )
                )
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
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            ),
            aggregations = aggregatorFactories
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

    fun `test transform with failure during indexing`() {
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

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
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            )
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
        if (!indexExists("transform-source-index")) {
            generateNYCTaxiData("transform-source-index")
            assertIndexExists("transform-source-index")
        }

        val aggregatorFactories = AggregatorFactories.builder()
        aggregatorFactories.addAggregator(
            ScriptedMetricAggregationBuilder("average_revenue_per_passenger_per_trip")
                .initScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "state.count = 0; state.sum = 0;", emptyMap()))
                .mapScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "state.sum += doc[\"random_field\"].value; state.count += doc[\"passenger_count\"].value",
                        emptyMap()
                    )
                )
                .combineScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "def d = new long[2]; d[0] = state.sum; d[1] = state.count; return d",
                        emptyMap()
                    )
                )
                .reduceScript(
                    Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "double sum = 0; double count = 0; for (a in states) { sum += a[0]; count += a[1]; } return sum/count",
                        emptyMap()
                    )
                )
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
                DateHistogram(sourceField = "tpep_pickup_datetime", targetField = "date", fixedInterval = "1d")
            ),
            aggregations = aggregatorFactories
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
                ContentType.APPLICATION_JSON
            )
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
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            )
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
        assertTrue("Doesn't capture indexed time", metadata.stats.indexTimeInMillis > 0)
        assertTrue("Didn't capture search time", metadata.stats.searchTimeInMillis > 0)
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
}
