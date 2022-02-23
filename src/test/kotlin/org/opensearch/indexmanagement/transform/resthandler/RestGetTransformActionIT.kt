/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.transform.TransformRestTestCase
import org.opensearch.indexmanagement.transform.action.get.GetTransformsRequest.Companion.DEFAULT_SIZE
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class RestGetTransformActionIT : TransformRestTestCase() {

    @Throws(Exception::class)
    fun `test getting a transform`() {
        var transform = createTransform(randomTransform())
        val indexedTransform = getTransform(transform.id)

        transform = transform.copy(
            schemaVersion = indexedTransform.schemaVersion,
            updatedAt = indexedTransform.updatedAt,
            jobSchedule = indexedTransform.jobSchedule,
            metadataId = null,
            // Roles are deprecated and will not be returned
            roles = listOf(),
            // User information is not returned as part of REST output
            user = null
        )
        assertEquals("Indexed and retrieved transform differ", transform, indexedTransform)
    }

    @Throws(Exception::class)
    fun `test getting a transform that doesn't exist`() {
        try {
            getTransform(OpenSearchTestCase.randomAlphaOfLength(20))
            fail("Expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test getting all transforms`() {
        val transforms = randomList(1, 15) { createTransform(randomTransform()) }

        // TODO: Delete existing transforms before test once delete API is available
        // Using a larger response size than the default in case leftover transforms prevent the ones created in this test from being returned
        val res = client().makeRequest("GET", "$TRANSFORM_BASE_URI?size=100")
        val map = res.asMap()
        val totalTransforms = map["total_transforms"] as Int
        val resTransforms = map["transforms"] as List<Map<String, Any?>>

        // There can be leftover transforms from previous tests, so we will have at least transforms.size or more
        assertTrue("Total transforms was not the same", transforms.size <= totalTransforms)
        assertTrue("Transform response has different size", transforms.size <= resTransforms.size)
        for (testTransform in transforms) {
            val foundTransform = resTransforms.find { testTransform.id == it["_id"] as String }
            assertNotNull("Did not find matching transform that should exist", foundTransform)
            val innerTransform = foundTransform!!["transform"] as Map<String, Any?>
            assertEquals(testTransform.id, foundTransform["_id"] as String)
            assertEquals(testTransform.seqNo, (foundTransform["_seq_no"] as Int).toLong())
            assertEquals(testTransform.primaryTerm, (foundTransform["_primary_term"] as Int).toLong())
            assertEquals(testTransform.id, innerTransform["transform_id"] as String)
            assertEquals(configSchemaVersion, innerTransform["schema_version"] as Int)
            assertEquals(testTransform.enabled, innerTransform["enabled"] as Boolean)
            assertEquals(testTransform.enabledAt?.toEpochMilli(), (innerTransform["enabled_at"] as Number?)?.toLong())

            assertEquals(testTransform.description, innerTransform["description"] as String)
            assertEquals(testTransform.sourceIndex, innerTransform["source_index"] as String)
            assertEquals(testTransform.targetIndex, innerTransform["target_index"] as String)
            assertNull(innerTransform["roles"])
            assertNull(innerTransform["user"])
            assertEquals(testTransform.pageSize, innerTransform["page_size"] as Int)
            assertEquals(testTransform.groups.size, (innerTransform["groups"] as List<Dimension>).size)
        }
    }

    @Throws(Exception::class)
    fun `test changing response size when getting transforms`() {
        val transformCount = 25
        repeat(transformCount) { createTransform(randomTransform()) }

        var res = client().makeRequest("GET", TRANSFORM_BASE_URI)
        var map = res.asMap()
        var resTransforms = map["transforms"] as List<Map<String, Any?>>

        assertEquals("Get transforms response returned an unexpected number of transforms", DEFAULT_SIZE, resTransforms.size)

        // Get Transforms with a larger response size
        res = client().makeRequest("GET", "$TRANSFORM_BASE_URI?size=$transformCount")
        map = res.asMap()
        resTransforms = map["transforms"] as List<Map<String, Any?>>

        assertEquals("Total transforms was not the same", transformCount, resTransforms.size)
    }

    @Throws(Exception::class)
    fun `test checking if a transform exists`() {
        val transform = createRandomTransform()
        val headResponse = client().makeRequest("HEAD", "$TRANSFORM_BASE_URI/${transform.id}")
        assertEquals("Unable to HEAD transform", RestStatus.OK, headResponse.restStatus())
        assertNull("Response contains unexpected body", headResponse.entity)
    }

    @Throws(Exception::class)
    fun `test checking if a non-existent transform exists`() {
        val headResponse = client().makeRequest("HEAD", "$TRANSFORM_BASE_URI/foobarbaz")
        assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
    }
}
