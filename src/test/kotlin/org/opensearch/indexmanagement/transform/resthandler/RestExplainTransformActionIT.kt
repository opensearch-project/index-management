/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.junit.Assert
import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.transform.TransformRestTestCase
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestExplainTransformActionIT : TransformRestTestCase() {

    @Throws(Exception::class)
    fun `test explain transform`() {
        val transform = randomTransform().copy(
            id = "test_explain_transform",
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            enabled = true,
            enabledAt = Instant.now(),
            metadataId = null,
            sourceIndex = "test_source",
            targetIndex = "test_target"
        ).let { createTransform(it, it.id) }
        updateTransformStartTime(transform)

        waitFor {
            val updatedTransform = getTransform(transformId = transform.id)
            assertNotNull("MetadataID on transform was null", updatedTransform.metadataId)
            val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/${updatedTransform.id}/_explain")
            assertEquals(RestStatus.OK, response.restStatus())
            val responseMap = response.asMap()
            assertNotNull("Response is null", responseMap)
            assertTrue("Response does not have metadata", responseMap.keys.isNotEmpty())
            val explainMetadata = responseMap[updatedTransform.id] as Map<String, Any>
            assertNotNull("Did not have key for transform ID", explainMetadata)
            assertEquals("Did not have metadata_id in explain response", updatedTransform.metadataId, explainMetadata["metadata_id"])
            val metadata = explainMetadata["transform_metadata"] as Map<String, Any>
            assertNotNull("Did not have metadata in explain response", metadata)
            if (!transform.continuous) {
                assertEquals("Status should be finished", TransformMetadata.Status.FINISHED.type, metadata["status"])
            } else {
                assertEquals("Status should be started", TransformMetadata.Status.STARTED.type, metadata["status"])
                assertNull("Should not have key for shard id to global checkpoint number map", metadata["shard_id_to_global_checkpoint"])
                assertNotNull("Did not have key for continuous transform stats", metadata["continuous_stats"])
                val continuousStats = metadata["continuous_stats"] as Map<String, Any>
                assertNotNull("Did not have last_timestamp in continuous transform stats", continuousStats["last_timestamp"])
                assertNotNull("Did not have documents_behind in continuous transform stats", continuousStats["documents_behind"])
                val documentsBehind = continuousStats["documents_behind"] as Map<String, Any>
                Assert.assertTrue("Continuous transform stats documents_behind did not contain the source index", documentsBehind.containsKey(transform.sourceIndex))
                Assert.assertEquals("Continuous transform stats should have 0 documents_behind", 0, documentsBehind[transform.sourceIndex])
            }
        }
    }

    @Throws(Exception::class)
    fun `test explain a transform with no id fails`() {
        try {
            val transform = randomTransform()
            client().makeRequest("GET", "$TRANSFORM_BASE_URI//_explain", emptyMap(), transform.toHttpEntity())
            fail("Expected 400 BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test explain transform for nonexistent id`() {
        // Creating a transform so the config index exists
        createTransform(transform = randomTransform(), transformId = "doesnt_exist_some_other_transform_id")
        val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/doesnt_exist/_explain")
        assertNull("Nonexistent transform didn't return null", response.asMap()["doesnt_exist"])
    }

    @Throws(Exception::class)
    fun `test explain transform for wildcard id`() {
        // Creating a transform so the config index exists
        createTransform(transform = randomTransform(), transformId = "wildcard_some_transform_id")
        createTransform(transform = randomTransform(), transformId = "wildcard_some_other_transform_id")
        val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/wildcard_some*/_explain")
        // We don't expect there to always be metadata we are creating random transforms and the job isn't running
        // but we do expect the wildcard some* to expand to the two jobs created above and have non-null values (meaning they exist)
        val map = response.asMap()
        assertNotNull("Non null wildcard_some_transform_id value wasn't in the response", map["wildcard_some_transform_id"])
        assertNotNull("Non null wildcard_some_other_transform_id value wasn't in the response", map["wildcard_some_other_transform_id"])
    }

    @Throws(Exception::class)
    fun `test explain continuous transform with wildcard id`() {
        val transform1 = randomTransform().copy(
            id = "continuous_wildcard_1",
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            enabled = true,
            enabledAt = Instant.now(),
            metadataId = null,
            continuous = true,
            pageSize = 50
        ).let { createTransform(it, it.id) }
        val transform2 = randomTransform().copy(
            id = "continuous_wildcard_2",
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            enabled = true,
            enabledAt = Instant.now(),
            metadataId = null,
            continuous = true,
            pageSize = 50
        ).let { createTransform(it, it.id) }

        updateTransformStartTime(transform1)
        updateTransformStartTime(transform2)

        waitFor {
            val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/continuous_wildcard*/_explain")
            assertEquals(RestStatus.OK, response.restStatus())
            val responseMap = response.asMap()
            listOf(transform1, transform2).forEach { transform ->
                val transformMetadata = (responseMap[transform.id] as Map<String, Any>)["transform_metadata"] as Map<String, Any>
                assertNotNull("Did not have metadata for transform ID", transformMetadata)
                val continuousStats = transformMetadata["continuous_stats"] as Map<String, Any>
                assertNotNull("Did not have last_timestamp in continuous transform stats", continuousStats["last_timestamp"])
                assertNotNull("Did not have documents_behind in continuous transform stats", continuousStats["documents_behind"])
                val documentsBehind = continuousStats["documents_behind"] as Map<String, Any>
                assertTrue("Source index was not contained in the documents behind explain response", documentsBehind.containsKey(transform.sourceIndex))
                assertEquals("Continuous transform stats should have 0 documents_behind", 0, documentsBehind[transform.sourceIndex])
                assertEquals("Documents behind should only display the transform source indices", 1, documentsBehind.size)
            }
        }
    }

    @Throws(Exception::class)
    fun `test explain transform for job that hasnt started`() {
        createTransform(transform = randomTransform().copy(metadataId = null), transformId = "not_started_some_transform_id")
        val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/not_started_some_transform_id/_explain")
        val expectedMap = mapOf("not_started_some_transform_id" to mapOf("metadata_id" to null, "transform_metadata" to null))
        assertEquals("The explain response did not match expected", expectedMap, response.asMap())
    }

    @Throws(Exception::class)
    fun `test explain transform for new transform attempted to create with metadata id`() {
        createTransform(transform = randomTransform().copy(metadataId = "some_metadata_id"), transformId = "no_meta_some_transform_id")
        val response = client().makeRequest("GET", "$TRANSFORM_BASE_URI/no_meta_some_transform_id/_explain")
        val expectedMap = mapOf("no_meta_some_transform_id" to mapOf("metadata_id" to null, "transform_metadata" to null))
        assertEquals("The explain response did not match expected", expectedMap, response.asMap())
    }

    @Throws(Exception::class)
    fun `test explain transform when config doesnt exist`() {
        deleteIndex(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
        val responseExplicit = client().makeRequest("GET", "$TRANSFORM_BASE_URI/no_config_some_transform/_explain")
        val expectedResponse = mapOf("no_config_some_transform" to "Failed to search transform metadata")
        assertEquals("Non-existent transform didn't return null", expectedResponse, responseExplicit.asMap())

        val responseExplicitMultiple = client().makeRequest("GET", "$TRANSFORM_BASE_URI/no_config_some_transform,no_config_another_transform/_explain")
        val expectedResponseMultiple = mapOf("no_config_some_transform" to "Failed to search transform metadata", "no_config_another_transform" to "Failed to search transform metadata")
        assertEquals("Non-existent transform didn't return null", expectedResponseMultiple, responseExplicitMultiple.asMap())

        val responseWildcard = client().makeRequest("GET", "$TRANSFORM_BASE_URI/no_config_another_*/_explain")
        val expectedResponseWildcard = mapOf<String, Map<String, Any>?>()
        assertEquals("Wildcard transform didn't return nothing", expectedResponseWildcard, responseWildcard.asMap())

        val responseMultipleTypes = client().makeRequest("GET", "$TRANSFORM_BASE_URI/no_config_some_transform,no_config_another_*/_explain")
        val expectedResponseMixed = mapOf("no_config_some_transform" to "Failed to search transform metadata")
        assertEquals("Non-existent and wildcard transform didn't return only non-existent with error", expectedResponseMixed, responseMultipleTypes.asMap())
    }
}
