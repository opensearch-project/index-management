/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.rest.RestStatus

class RestStartSnapshotManagementIT : SnapshotManagementRestTestCase() {

    fun `test starting a stopped snapshot management policy`() {
        val smPolicy = createSMPolicy(randomSMPolicy().copy(jobEnabled = false, jobEnabledTime = null))
        assertFalse("Snapshot management policy was not disabled", smPolicy.jobEnabled)

        val response = client().makeRequest("PUT", "${IndexManagementPlugin.SM_POLICIES_URI}/${smPolicy.getSMPolicyName()}/_start")
        assertEquals("Start snapshot management policy failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedSMPolicy = getSMPolicy(smPolicy.getSMPolicyName())
        assertTrue("Snapshot management policy was not enabled", updatedSMPolicy.jobEnabled)
    }

    fun `test starting a snapshot management policy with an invalid id fails`() {
        // Test with no ID
        try {
            client().makeRequest("PUT", "${IndexManagementPlugin.SM_POLICIES_URI}//_start")
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
        // Test with a nonexistent ID
        try {
            client().makeRequest("PUT", "${IndexManagementPlugin.SM_POLICIES_URI}/${randomAlphaOfLength(20)}/_start")
            fail("Expected NOT_FOUND response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
