/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.resthandler

import org.apache.http.HttpHeaders
import org.apache.http.message.BasicHeader
import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.rest.RestStatus

class RestGetSnapshotManagementIT : SnapshotManagementRestTestCase() {

    fun `test getting a snapshot management policy`() {
        var smPolicy = createSMPolicy(randomSMPolicy().copy(jobEnabled = false, jobEnabledTime = null))
        val indexedSMPolicy = getSMPolicy(smPolicy.policyName)
        // Schema version and last updated time are updated during the creation so we need to update the original too for comparison
        // Job schedule interval will have a dynamic start time
        smPolicy = smPolicy.copy(
            id = indexedSMPolicy.id,
            seqNo = indexedSMPolicy.seqNo,
            primaryTerm = indexedSMPolicy.primaryTerm,
            jobLastUpdateTime = indexedSMPolicy.jobLastUpdateTime,
            jobSchedule = indexedSMPolicy.jobSchedule
        )
        assertEquals("Indexed and retrieved snapshot management policies differ", smPolicy, indexedSMPolicy)
    }

    @Throws(Exception::class)
    fun `test getting a snapshot management policy that doesn't exist`() {
        try {
            getSMPolicy(randomAlphaOfLength(20))
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test getting a snapshot management policy that doesn't exist and config index doesnt exist`() {
        try {
            deleteIndex(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
            getSMPolicy(randomAlphaOfLength(20))
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    @Suppress("UNCHECKED_CAST")
    fun `test getting all snapshot management policies`() {
        val smPolicies = randomList(1, 15) { createSMPolicy(randomSMPolicy()) }
        val response = client().makeRequest(
            "GET", "${IndexManagementPlugin.SM_POLICIES_URI}/", null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        val map = response.asMap()
        val totalPolicies = map["total_policies"] as Int
        val responsePolicies = map["policies"] as List<Map<String, Any?>>
        assertTrue("Total policies was not the same", smPolicies.size <= totalPolicies)
        assertTrue("SM Policies response has different size", smPolicies.size <= responsePolicies.size)
        for (testSMPolicy in smPolicies) {
            val foundPolicy = responsePolicies.find { testSMPolicy.id == it["_id"] as String }
            assertNotNull("Did not find matching SM Policy that should exist", foundPolicy)
            // TODO check other properties
        }
    }
}
