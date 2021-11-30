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
class RestStartTransformActionIT : TransformRestTestCase() {

    @Throws(Exception::class)
    fun `test starting a stopped transform`() {
        val transform = createTransform(randomTransform().copy(enabled = false, enabledAt = null, metadataId = null))
        assertFalse("Transform was not disabled", transform.enabled)

        val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_start")
        assertEquals("Start transform failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedTransform = getTransform(transform.id)
        assertTrue("Transform was not enabled", updatedTransform.enabled)
    }

    @Throws(Exception::class)
    fun `test starting a started transform doesnt change enabled time`() {
        // First create a non-started transform
        val transform = createTransform(randomTransform().copy(enabled = false, enabledAt = null, metadataId = null))
        assertTrue("Transform was not disabled", !transform.enabled)

        // Enable it to get the job enabled time
        val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_start")
        assertEquals("Start transform failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedTransform = getTransform(transform.id)
        assertTrue("Transform was not enabled", updatedTransform.enabled)

        val secondResponse = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_start")
        assertEquals("Start transform failed", RestStatus.OK, secondResponse.restStatus())
        val expectedSecondResponse = mapOf("acknowledged" to true)
        assertEquals(expectedSecondResponse, secondResponse.asMap())

        // Confirm the job enabled time is not reset to a newer time if job was already enabled
        val updatedSecondTransform = getTransform(transform.id)
        assertTrue("Transform was not enabled", updatedSecondTransform.enabled)
        assertEquals("Jobs had different enabled times", updatedTransform.enabledAt, updatedSecondTransform.enabledAt)
    }

    @Throws(Exception::class)
    fun `test start a transform with no id fails`() {
        try {
            client().makeRequest("POST", "$TRANSFORM_BASE_URI//_start")
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test starting a failed transform`() {
        val transform = randomTransform().copy(
            id = "restart_failed_transform",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            updatedAt = Instant.now(),
            enabledAt = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_restart_failed_transform",
            targetIndex = "target_restart_failed_transform",
            metadataId = null,
            roles = emptyList(),
            pageSize = 10,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag"),
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")
            ),
            aggregations = AggregatorFactories.builder()
        ).let { createTransform(it, it.id) }

        // This should fail because source index is deleted
        deleteIndex(transform.sourceIndex)
        updateTransformStartTime(transform)

        waitFor {
            val updatedTransform = getTransform(transformId = transform.id)
            assertNotNull("MetadataID on transform was null", updatedTransform.metadataId)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            // It should be failed because we did not create the source index
            assertEquals("Status should be failed", TransformMetadata.Status.FAILED, metadata.status)
            assertFalse("Transform was not disabled", updatedTransform.enabled)
        }

        // Now create the missing source index
        generateNYCTaxiData("source_restart_failed_transform")
        assertIndexExists("source_restart_failed_transform")

        // And call _start on the failed transform job
        val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_start")
        assertEquals("Start transform failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedTransform = getTransform(transform.id)
        assertTrue("Transform was not enabled", updatedTransform.enabled)
        waitFor {
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            // Metadata should be started now
            assertEquals("Status should be started", TransformMetadata.Status.STARTED, metadata.status)
        }

        updateTransformStartTime(transform)

        // Transform should be able to finish, with actual transformed docs
        waitFor {
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            if (transform.continuous) {
                assertEquals("Status should be started", TransformMetadata.Status.STARTED, metadata.status)
            } else {
                assertEquals("Status should be finished", TransformMetadata.Status.FINISHED, metadata.status)
            }
            assertEquals("Did not transform documents", 5000, metadata.stats.documentsProcessed)
            assertTrue("Did not transform documents", metadata.stats.documentsIndexed > 0)
        }
        if (transform.continuous) disableTransform(transform.id)
    }

    @Throws(Exception::class)
    fun `test starting a finished transform`() {
        generateNYCTaxiData("source_restart_finished_transform")
        assertIndexExists("source_restart_finished_transform")
        val transform = randomTransform().copy(
            id = "restart_finished_transform",
            schemaVersion = 1L,
            enabled = true,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            updatedAt = Instant.now(),
            enabledAt = Instant.now(),
            description = "basic search test",
            sourceIndex = "source_restart_finished_transform",
            targetIndex = "target_restart_finished_transform",
            metadataId = null,
            roles = emptyList(),
            pageSize = 10,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            ),
            aggregations = AggregatorFactories.builder(),
            continuous = false
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform)
        var firstTransformsIndexed = 0L
        waitFor {
            val updatedTransform = getTransform(transformId = transform.id)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            assertEquals("Status should be finished", TransformMetadata.Status.FINISHED, metadata.status)
            assertEquals("Did not transform documents", 5000, metadata.stats.documentsProcessed)
            assertTrue("Did not transform documents", metadata.stats.documentsIndexed > 0)
            firstTransformsIndexed = metadata.stats.documentsIndexed
        }

        deleteIndex("target_restart_finished_transform")

        // And call _start on the finished transform job
        val response = client().makeRequest("POST", "$TRANSFORM_BASE_URI/${transform.id}/_start")
        assertEquals("Start transform failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        updateTransformStartTime(transform)

        // Transform should be able to be finished, with actual transformed docs again
        waitFor {
            val updatedTransform = getTransform(transformId = transform.id)
            val metadata = getTransformMetadata(updatedTransform.metadataId!!)
            assertEquals("Status should be finished", TransformMetadata.Status.FINISHED, metadata.status)
            assertEquals("Did not transform documents", 10000, metadata.stats.documentsProcessed)
            assertEquals("Did not index transform docs", firstTransformsIndexed * 2, metadata.stats.documentsIndexed)
            assertIndexExists("target_restart_finished_transform")
        }
    }

    @Throws(Exception::class)
    fun `test start a transform that does not exist fails`() {
        try {
            client().makeRequest("POST", "$TRANSFORM_BASE_URI/does_not_exist/_start")
            fail("Expected 400 Method NOT FOUND response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
