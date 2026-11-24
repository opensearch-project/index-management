/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILED_INDICES
import org.opensearch.indexmanagement.indexstatemanagement.util.FAILURES
import org.opensearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class RestExecuteManagedIndexActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    private fun execute(indexName: String) = client().makeRequest(
        RestRequest.Method.POST.toString(),
        "${RestExecuteManagedIndexAction.EXECUTE_BASE_URI}/$indexName",
    )

    private fun assertExecuted(indexName: String) {
        val response = execute(indexName).asMap()
        assertEquals("Unexpected response: $response", 1, response[UPDATED_INDICES])
    }

    fun `test missing index`() {
        try {
            client().makeRequest(RestRequest.Method.POST.toString(), "${RestExecuteManagedIndexAction.EXECUTE_BASE_URI}/${testIndexName}_missing")
            fail("Expected a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test wildcard is rejected`() {
        try {
            execute("$testIndexName*")
            fail("Expected a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test index that is not managed`() {
        val indexName = "${testIndexName}_not_managed"
        createIndex(indexName, null)

        val response = execute(indexName)
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        val expected =
            mapOf(
                FAILURES to true,
                UPDATED_INDICES to 0,
                FAILED_INDICES to
                    listOf(
                        mapOf(
                            "index_name" to indexName,
                            "index_uuid" to getUuid(indexName),
                            "reason" to "This index is not being managed.",
                        ),
                    ),
            )
        assertAffectedIndicesResponseIsEqual(expected, response.asMap())
    }

    fun `test execute runs managed index without waiting for the schedule`() {
        val indexName = "${testIndexName}_basic"
        val policyID = "${testIndexName}_basic_policy"
        val states = listOf(State("DeleteState", listOf(DeleteAction(0)), listOf()))
        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID)
        waitFor { assertIndexExists(indexName) }
        getExistingManagedIndexConfig(indexName)

        // The first execution initializes the managed index
        waitFor { assertExecuted(indexName) }
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // The second execution runs the delete action
        waitFor { assertExecuted(indexName) }
        waitFor { assertIndexDoesNotExist(indexName) }
    }
}
