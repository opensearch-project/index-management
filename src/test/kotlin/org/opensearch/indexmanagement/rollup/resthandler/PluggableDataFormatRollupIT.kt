/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class PluggableDataFormatRollupIT : RollupRestAPITestCase() {

    fun `test rollup creation fails on pluggable dataformat index`() {
        val sourceIndex = "pluggable-rollup-source"
        val settings = Settings.builder().put("index.pluggable.dataformat.enabled", true).build()
        createIndex(sourceIndex, settings, """"properties": {"timestamp": {"type": "date"}, "value": {"type": "long"}}""")

        val rollup = randomRollup().copy(sourceIndex = sourceIndex, targetIndex = "pluggable-rollup-target")
        try {
            client().makeRequest("PUT", "$ROLLUP_JOBS_BASE_URI/${rollup.id}", emptyMap(), rollup.toHttpEntity())
            fail("Expected 400 BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            assertTrue(
                "Error should mention Optimized Engine",
                e.message!!.contains("Rollup is not supported with Optimized Engine currently"),
            )
        }
    }

    fun `test rollup creation succeeds on normal index`() {
        val rollup = randomRollup()
        createRollupSourceIndex(rollup)
        val response = client().makeRequest("PUT", "$ROLLUP_JOBS_BASE_URI/${rollup.id}", emptyMap(), rollup.toHttpEntity())
        assertEquals("Create rollup failed", RestStatus.CREATED, response.restStatus())
    }
}
