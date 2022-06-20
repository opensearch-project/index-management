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
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.ENABLED_TIME_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy.Companion.SM_TYPE
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
            "GET", IndexManagementPlugin.SM_POLICIES_URI, null,
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
            assertNotNull("Matching response did not have policy", foundPolicy?.get(SM_TYPE))
            val policyInResponse = foundPolicy!![SM_TYPE]
            val actualPolicyMap = testSMPolicy.convertToMap()[SM_TYPE] as MutableMap<String, Any>
            actualPolicyMap.remove(SMPolicy.USER_FIELD)
            assertEquals("Policy in get all response differed from actual", actualPolicyMap, policyInResponse)
        }
    }

    @Throws(Exception::class)
    @Suppress("UNCHECKED_CAST")
    fun `test getting all snapshot management policies with search params`() {
        var policyID = 0
        val smPolicies = randomList(10, 10) { createSMPolicy(randomSMPolicy(policyName = (policyID++).toString())) }
        val searchParamSize = 5
        val searchParamQuery = mutableMapOf("size" to searchParamSize.toString(), "queryString" to "0 OR 1 OR 2 OR 3 OR 4 OR 5 OR 6 OR 7 OR 8")
        val response = client().makeRequest("GET", IndexManagementPlugin.SM_POLICIES_URI, searchParamQuery, null)
        val map = response.asMap()
        val totalPolicies = map["total_policies"] as Int
        val responsePolicies = map["policies"] as List<Map<String, Any?>>
        // We expect 9 policies due to the name designation in the query string query
        assertEquals("Total policies was not the same", 9, totalPolicies)
        assertEquals("SM Policies response has different size", searchParamSize, responsePolicies.size)
        for (testSMPolicy in smPolicies.subList(0, searchParamSize)) {
            val foundPolicy = responsePolicies.find { testSMPolicy.id == it["_id"] as String }
            assertNotNull("Did not find matching SM Policy that should exist", foundPolicy)
            assertNotNull("Matching response did not have policy", foundPolicy?.get(SM_TYPE))
            val policyInResponse = foundPolicy!![SM_TYPE]
            val actualPolicyMap = testSMPolicy.convertToMap()[SM_TYPE] as MutableMap<String, Any>
            actualPolicyMap.remove(SMPolicy.USER_FIELD)
            assertEquals("Policy in get all response differed from actual", actualPolicyMap, policyInResponse)
        }
        // Add the 'from' param
        searchParamQuery["from"] = searchParamSize.toString()
        // Repeat the query while specifying "from" to get the remaining results
        val responseFrom = client().makeRequest("GET", IndexManagementPlugin.SM_POLICIES_URI, searchParamQuery, null)
        val mapFrom = responseFrom.asMap()
        val totalPoliciesFrom = mapFrom["total_policies"] as Int
        assertEquals("Total policies was not the same", 9, totalPoliciesFrom)
        val responsePoliciesFrom = mapFrom["policies"] as List<Map<String, Any?>>
        assertEquals("SM Policies response has different size", 9 - searchParamSize, responsePoliciesFrom.size)
        for (testSMPolicy in smPolicies.subList(searchParamSize, 9)) {
            val foundPolicy = responsePoliciesFrom.find { testSMPolicy.id == it["_id"] as String }
            assertNotNull("Did not find matching SM Policy that should exist", foundPolicy)
            assertNotNull("Matching response did not have policy", foundPolicy?.get(SM_TYPE))
            val policyInResponse = foundPolicy!![SM_TYPE]
            val actualPolicyMap = testSMPolicy.convertToMap()[SM_TYPE] as MutableMap<String, Any>
            actualPolicyMap.remove(SMPolicy.USER_FIELD)
            assertEquals("Policy in get all response differed from actual", actualPolicyMap, policyInResponse)
        }
    }

    @Throws(Exception::class)
    @Suppress("UNCHECKED_CAST")
    fun `test getting all snapshot management policies with sort order`() {
        val smPolicies = randomList(5, 5) { createSMPolicy(randomSMPolicy(jobEnabled = true)) }
        val sortedPolicies = smPolicies.sortedByDescending { it.jobEnabledTime }
        val searchParamQuery = mutableMapOf("sortOrder" to "desc", "sortField" to "$SM_TYPE.$ENABLED_TIME_FIELD")
        val response = client().makeRequest("GET", IndexManagementPlugin.SM_POLICIES_URI, searchParamQuery, null)
        val map = response.asMap()
        val totalPolicies = map["total_policies"] as Int
        val responsePolicies = map["policies"] as List<Map<String, Any?>>
        assertEquals("Total policies was not the same", smPolicies.size, totalPolicies)
        assertEquals("SM Policies response has different size", smPolicies.size, responsePolicies.size)
        for (policyNumber in sortedPolicies.indices) {
            val actualPolicy = sortedPolicies[policyNumber]
            val foundPolicy = responsePolicies[policyNumber]
            val policyInResponse = foundPolicy[SM_TYPE]
            val actualPolicyMap = actualPolicy.convertToMap()[SM_TYPE] as MutableMap<String, Any>
            actualPolicyMap.remove(SMPolicy.USER_FIELD)
            assertEquals("Policy in get all response differed from actual", actualPolicyMap, policyInResponse)
        }
    }
}
