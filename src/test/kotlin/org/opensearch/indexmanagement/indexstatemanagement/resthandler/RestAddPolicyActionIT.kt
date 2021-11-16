/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILED_INDICES
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILURES
import org.opensearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestStatus

class RestAddPolicyActionIT : IndexStateManagementRestTestCase() {

    fun `test missing indices`() {
        try {
            client().makeRequest(POST.toString(), RestAddPolicyAction.ADD_POLICY_BASE_URI)
            fail("Expected a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "illegal_argument_exception", "reason" to "Missing indices")
                    ),
                    "type" to "illegal_argument_exception",
                    "reason" to "Missing indices"
                ),
                "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test closed index`() {
        val index = "movies"
        val policy = createRandomPolicy()
        createIndex(index, null)
        closeIndex(index)

        val response = client().makeRequest(
            POST.toString(),
            "${RestAddPolicyAction.ADD_POLICY_BASE_URI}/$index",
            StringEntity("{ \"policy_id\": \"${policy.id}\" }", APPLICATION_JSON)
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to index,
                    "index_uuid" to getUuid(index),
                    "reason" to "This index is closed"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)
    }

    fun `test index with existing policy`() {
        val index = "movies"
        val policy = createRandomPolicy()
        createIndex(index, policy.id)

        val response = client().makeRequest(
            POST.toString(),
            "${RestAddPolicyAction.ADD_POLICY_BASE_URI}/$index",
            StringEntity("{ \"policy_id\": \"${policy.id}\" }", APPLICATION_JSON)
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to index,
                    "index_uuid" to getUuid(index),
                    "reason" to "This index already has a policy, use the update policy API to update index policies"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)
    }

    fun `test index list`() {
        val indexOne = "movies_1"
        val indexTwo = "movies_2"
        val policy = createRandomPolicy()
        val newPolicy = createRandomPolicy()
        createIndex(indexOne, null)
        createIndex(indexTwo, policy.id)

        closeIndex(indexOne)

        val response = client().makeRequest(
            POST.toString(),
            "${RestAddPolicyAction.ADD_POLICY_BASE_URI}/$indexOne,$indexTwo",
            StringEntity("{ \"policy_id\": \"${newPolicy.id}\" }", APPLICATION_JSON)
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            FAILURES to true,
            UPDATED_INDICES to 0,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexOne,
                    "index_uuid" to getUuid(indexOne),
                    "reason" to "This index is closed"
                ),
                mapOf(
                    "index_name" to indexTwo,
                    "index_uuid" to getUuid(indexTwo),
                    "reason" to "This index already has a policy, use the update policy API to update index policies"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)
    }

    fun `test index pattern`() {
        val indexPattern = "movies"
        val indexOne = "movies_1"
        val indexTwo = "movies_2"
        val indexThree = "movies_3"
        val policy = createRandomPolicy()
        val newPolicy = createRandomPolicy()
        createIndex(indexOne, null)
        createIndex(indexTwo, policy.id)
        createIndex(indexThree, null)

        closeIndex(indexOne)

        val response = client().makeRequest(
            POST.toString(),
            "${RestAddPolicyAction.ADD_POLICY_BASE_URI}/$indexPattern*",
            StringEntity("{ \"policy_id\": \"${newPolicy.id}\" }", APPLICATION_JSON)
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            UPDATED_INDICES to 1,
            FAILURES to true,
            FAILED_INDICES to listOf(
                mapOf(
                    "index_name" to indexOne,
                    "index_uuid" to getUuid(indexOne),
                    "reason" to "This index is closed"
                ),
                mapOf(
                    "index_name" to indexTwo,
                    "index_uuid" to getUuid(indexTwo),
                    "reason" to "This index already has a policy, use the update policy API to update index policies"
                )
            )
        )

        assertAffectedIndicesResponseIsEqual(expectedMessage, actualMessage)

        // Check if indexThree had policy set
        waitFor {
            assertEquals(newPolicy.id, getPolicyIDOfManagedIndex(indexThree))
        }
    }
}
