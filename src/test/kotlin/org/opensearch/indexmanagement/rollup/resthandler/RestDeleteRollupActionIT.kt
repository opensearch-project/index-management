/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.indexstatemanagement.wait
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
class RestDeleteRollupActionIT : RollupRestAPITestCase() {

    @Throws(Exception::class)
    fun `test deleting a rollup`() {
        val rollup = createRandomRollup()

        val deleteResponse = client().makeRequest("DELETE", "$ROLLUP_JOBS_BASE_URI/${rollup.id}?refresh=true")
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        val getResponse = client().makeRequest("HEAD", "$ROLLUP_JOBS_BASE_URI/${rollup.id}")
        assertEquals("Deleted rollup still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    @Throws(Exception::class)
    fun `test deleting a rollup that doesn't exist in existing config index`() {
        createRandomRollup()
        wait {
            try {
                client().makeRequest("DELETE", "$ROLLUP_JOBS_BASE_URI/foobarbaz")
                fail("expected 404 ResponseException")
            } catch (e: ResponseException) {
                assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
            }
        }
    }

    @Throws(Exception::class)
    fun `test deleting a rollup that doesn't exist and config index doesnt exist`() {
        try {
            wipeAllIndices()
            client().makeRequest("DELETE", "$ROLLUP_JOBS_BASE_URI/foobarbaz")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
