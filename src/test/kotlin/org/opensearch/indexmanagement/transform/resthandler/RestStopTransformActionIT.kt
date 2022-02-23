/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.transform.TransformRestTestCase
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestStopTransformActionIT : TransformRestTestCase() {

    @Throws(Exception::class)
    fun `test stopping a stopped Transform`() {
        val transform = createTransform(randomTransform().copy(enabled = true, enabledAt = randomInstant(), metadataId = null))
        assertTrue("Transform was not enabled", transform.enabled)

        val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
        assertEquals("Stop transform failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedTransform = getTransform(transform.id)
        assertFalse("Transform was not disabled", updatedTransform.enabled)

        val secondResponse = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
        assertEquals("Stop transform failed", RestStatus.OK, secondResponse.restStatus())
        val expectedSecondResponse = mapOf("acknowledged" to true)
        assertEquals(expectedSecondResponse, secondResponse.asMap())

        val updatedSecondTransform = getTransform(transform.id)
        assertFalse("Transform was not disabled", updatedSecondTransform.enabled)
    }

    @Throws(Exception::class)
    fun `test stopping a finished transform`() {
        // Create a transform that finishes
        val transform = createTransform(
            randomTransform()
                .copy(
                    jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
                    enabled = true,
                    enabledAt = Instant.now(),
                    metadataId = null,
                    continuous = false
                )
        )
        updateTransformStartTime(transform)

        // Assert it finished
        waitFor {
            val updatedTransform = getTransform(transform.id)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            assertEquals("Transform never finished", TransformMetadata.Status.FINISHED, metadata.status)
            // Waiting for job to be disabled here to avoid version conflict exceptions later on
            assertFalse("Job was not disabled", updatedTransform.enabled)
        }

        // Try to stop a finished transform
        val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
        assertEquals("Stop transform failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        // Assert it is still in finished status
        waitFor {
            val updatedTransform = getTransform(transform.id)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            assertEquals("Transform should have stayed finished", TransformMetadata.Status.FINISHED, metadata.status)
        }
    }

    @Throws(Exception::class)
    fun `test stopping a failed transform`() {
        // Create a transform that will fail because no source index
        val transform = randomTransform().copy(
            id = "test_stopping_a_failed_transform",
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            enabled = true,
            enabledAt = Instant.now(),
            metadataId = null
        ).let { createTransform(it, it.id) }
        deleteIndex(transform.sourceIndex)
        updateTransformStartTime(transform)

        // Assert it's in failed
        waitFor {
            val updatedTransform = getTransform(transform.id)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            assertEquals("Transform never failed", TransformMetadata.Status.FAILED, metadata.status)
        }

        waitFor {
            // Stop transform
            val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
            assertEquals("Stop transform failed", RestStatus.OK, response.restStatus())
            val expectedResponse = mapOf("acknowledged" to true)
            assertEquals(expectedResponse, response.asMap())
        }

        // Assert transform still failed status
        waitFor {
            val updatedTransform = getTransform(transform.id)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            assertEquals("Transform should have stayed failed", TransformMetadata.Status.FAILED, metadata.status)
        }
    }

    // ISSUE: Goes straight to failed before test can pick up STARTED status

    @Throws(Exception::class)
    fun `test stopping a running transform`() {
        generateNYCTaxiData("source_test_stop_running_transform")
        val transform = randomTransform().copy(
            id = "test_stop_running_transform",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            updatedAt = Instant.now(),
            enabledAt = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_test_stop_running_transform",
            targetIndex = "target_test_stop_running_transform",
            metadataId = null,
            roles = emptyList(),
            pageSize = 1,
            groups = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1m"),
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            ),
            aggregations = AggregatorFactories.builder()
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)

        waitFor {
            val transformJob = getTransform(transformId = transform.id)
            assertNotNull("Transform job doesn't have metadata set", transformJob.metadataId)
            val transformMetadata = getTransformMetadata(transformJob.metadataId!!)
            assertEquals("Transform is not STARTED", TransformMetadata.Status.STARTED, transformMetadata.status)

            // There are two calls to _stop happening serially which is prone to version conflicts during an ongoing job
            // so including it in a waitFor to ensure it can retry a few times
            val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_stop")
            assertEquals("Stop transform failed", RestStatus.OK, response.restStatus())
            val expectedResponse = mapOf("acknowledged" to true)
            assertEquals(expectedResponse, response.asMap())
        }

        val updatedTransform = getTransform(transform.id)
        assertFalse("Transform was not disabled", updatedTransform.enabled)
        val transformMetadata = getTransformMetadata(updatedTransform.metadataId!!)
        assertEquals("Transform is not STOPPED", TransformMetadata.Status.STOPPED, transformMetadata.status)
    }

    @Throws(Exception::class)
    fun `test stop a transform with no id fails`() {
        try {
            client().makeRequest("POST", "$TRANSFORM_BASE_URI//_stop")
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test stopping a transform that doesn't exist`() {
        try {
            client().makeRequest("POST", "$TRANSFORM_BASE_URI/does_not_exist/_stop")
            fail("Expected 400 Method NOT_FOUND response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
