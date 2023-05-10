/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.junit.AfterClass
import org.junit.Before
import org.opensearch.client.ResponseException
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.transform.TransformRestTestCase
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.AggregatorFactories
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit

@Suppress("UNCHECKED_CAST")
class RestPreviewTransformActionIT : TransformRestTestCase() {

    private val factories = AggregatorFactories.builder()
        .addAggregator(AggregationBuilders.sum("revenue").field("total_amount"))
        .addAggregator(AggregationBuilders.percentiles("passengerCount").field("passenger_count").percentiles(90.0, 95.0))
    private val transform = randomTransform().copy(
        sourceIndex = sourceIndex,
        groups = listOf(
            Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
        ),
        aggregations = factories
    )

    @Before
    fun setupData() {
        var indexExists = false
        try {
            indexExists = indexExists(sourceIndex)
        } catch (e: IndexNotFoundException) {
            // If this exception is thrown, indexExists will be false anyways
        }
        if (!indexExists) {
            generateNYCTaxiData(sourceIndex)
        }
    }

    companion object {
        private const val sourceIndex = "transform-preview-api"

        @AfterClass
        @JvmStatic
        fun deleteData() {
            deleteIndex(sourceIndex)
        }
    }

    fun `test preview`() {
        val response = client().makeRequest(
            "POST",
            "$TRANSFORM_BASE_URI/_preview",
            emptyMap(),
            transform.toHttpEntity()
        )
        val expectedKeys = setOf("revenue", "passengerCount", "flag", "transform._doc_count", "_doc_count")
        assertEquals("Preview transform failed", RestStatus.OK, response.restStatus())
        val transformedDocs = response.asMap()["documents"] as List<Map<String, Any>>
        assertEquals("Transformed docs have unexpected schema", expectedKeys, transformedDocs.first().keys)
    }

    fun `test preview with term aggregation on date field`() {
        val targetIdxTestName = "target_idx_test_14"
        val pickupDateTime = "tpep_pickup_datetime"
        val fareAmount = "fare_amount"

        val transform = Transform(
            id = "id_14",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform doc values must be the same",
            metadataId = null,
            sourceIndex = sourceIndex,
            targetIndex = targetIdxTestName,
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                Terms(sourceField = pickupDateTime, targetField = pickupDateTime)
            ),
            aggregations = AggregatorFactories.builder().addAggregator(AggregationBuilders.avg(fareAmount).field(fareAmount))
        ).let { createTransform(it, it.id) }

        val response = client().makeRequest(
            "POST",
            "$TRANSFORM_BASE_URI/_preview",
            emptyMap(),
            transform.toHttpEntity()
        )
        val expectedKeys = setOf("fare_amount", "tpep_pickup_datetime", "transform._doc_count", "_doc_count")
        assertEquals("Preview transform failed", RestStatus.OK, response.restStatus())
        val transformedDocs = response.asMap()["documents"] as List<Map<String, Any>>
        assertEquals("Transformed docs have unexpected schema", expectedKeys, transformedDocs.first().keys)

        val dateFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSZZ").withZone(ZoneId.of("UTC"))
        for (doc in transformedDocs) {
            assertTrue(isValid(doc["tpep_pickup_datetime"] as? String, dateFormatter))
        }
    }

    fun `test mismatched columns`() {
        val factories = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.sum("revenue").field("total_amountdzdfd"))
        val transform = transform.copy(
            groups = listOf(Terms(sourceField = "non-existent", targetField = "non-existent")),
            aggregations = factories
        )
        try {
            client().makeRequest(
                "POST",
                "$TRANSFORM_BASE_URI/_preview",
                emptyMap(),
                transform.toHttpEntity()
            )
            fail("expected exception")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test nonexistent source index`() {
        val transform = transform.copy(sourceIndex = "non-existent-index")
        try {
            client().makeRequest(
                "POST",
                "$TRANSFORM_BASE_URI/_preview",
                emptyMap(),
                transform.toHttpEntity()
            )
            fail("expected exception")
        } catch (e: ResponseException) {
            assertEquals("Unexpected failure code", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    private fun isValid(dateStr: String?, dateFormatter: DateTimeFormatter): Boolean {
        try {
            LocalDate.parse(dateStr, dateFormatter)
        } catch (e: DateTimeParseException) {
            return false
        }
        return true
    }
}
