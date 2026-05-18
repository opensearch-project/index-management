/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.transform.TransformRestTestCase
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class PluggableDataFormatTransformIT : TransformRestTestCase() {

    fun `test transform creation fails on pluggable dataformat index`() {
        val sourceIndex = "pluggable-transform-source"
        val settings = Settings.builder().put("index.pluggable.dataformat.enabled", true).build()
        createIndex(sourceIndex, settings, """"properties": {"timestamp": {"type": "date"}, "value": {"type": "long"}}""")

        val transform = randomTransform().copy(sourceIndex = sourceIndex, targetIndex = "pluggable-transform-target")
        try {
            client().makeRequest("PUT", "$TRANSFORM_BASE_URI/${transform.id}", emptyMap(), transform.toHttpEntity())
            fail("Expected 400 BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            assertTrue(
                "Error should mention Optimized Engine",
                e.message!!.contains("Transform is not supported with Optimized Engine currently"),
            )
        }
    }

    fun `test transform creation succeeds on normal index`() {
        val transform = randomTransform()
        createTransformSourceIndex(transform)
        val response = client().makeRequest("PUT", "$TRANSFORM_BASE_URI/${transform.id}", emptyMap(), transform.toHttpEntity())
        assertEquals("Create transform failed", RestStatus.CREATED, response.restStatus())
    }
}
