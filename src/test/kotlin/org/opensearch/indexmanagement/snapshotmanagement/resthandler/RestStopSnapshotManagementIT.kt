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
import java.time.Instant

class RestStopSnapshotManagementIT : SnapshotManagementRestTestCase() {

    fun `test stopping an enabled snapshot management policy`() {
        val smPolicy = createSMPolicy(randomSMPolicy().copy(jobEnabled = true, jobEnabledTime = Instant.now()))
        assertTrue("Snapshot management policy was not enabled", smPolicy.jobEnabled)

        val response = client().makeRequest("POST", "${IndexManagementPlugin.SM_POLICIES_URI}/${smPolicy.policyName}/_stop")
        assertEquals("Stop snapshot management policy failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedSMPolicy = getSMPolicy(smPolicy.policyName)
        assertFalse("Snapshot management policy was not disabled", updatedSMPolicy.jobEnabled)
    }

    fun `test stopping a disabled snapshot management policy`() {
        val smPolicy = createSMPolicy(randomSMPolicy().copy(jobEnabled = false))
        assertFalse("Snapshot management policy should not be enabled", smPolicy.jobEnabled)

        val response = client().makeRequest("POST", "${IndexManagementPlugin.SM_POLICIES_URI}/${smPolicy.policyName}/_stop")
        assertEquals("Stop snapshot management policy failed", RestStatus.OK, response.restStatus())
        val expectedResponse = mapOf("acknowledged" to true)
        assertEquals(expectedResponse, response.asMap())

        val updatedSMPolicy = getSMPolicy(smPolicy.policyName)
        assertFalse("Snapshot management policy was not disabled", updatedSMPolicy.jobEnabled)
    }

    fun `test stopping a snapshot management policy with an invalid id fails`() {
        // Test with no ID
        try {
            client().makeRequest("POST", "${IndexManagementPlugin.SM_POLICIES_URI}//_stop")
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
        // Test with a nonexistent ID
        try {
            client().makeRequest("POST", "${IndexManagementPlugin.SM_POLICIES_URI}/${randomAlphaOfLength(20).lowercase()}/_stop")
            fail("Expected NOT_FOUND response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test stopping a snapshot management policy with no config index fails`() {
        try {
            deleteIndex(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
            client().makeRequest("POST", "${IndexManagementPlugin.SM_POLICIES_URI}/nonexistent_foo/_stop")
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
