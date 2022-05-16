/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.opensearch.action.search.SearchResponse
import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent.jsonXContent
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadOnlyAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomReadOnlyActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging(value = "level:DEBUG", reason = "Debugging tests")
@Suppress("UNCHECKED_CAST")
class IndexStateManagementRestApiIT : IndexStateManagementRestTestCase() {

    @Throws(Exception::class)
    fun `test plugins are loaded`() {
        val response = entityAsMap(client().makeRequest("GET", "_nodes/plugins"))
        val nodesInfo = response["nodes"] as Map<String, Map<String, Any>>
        var hasIndexStateManagementPlugin = false
        var hasJobSchedulerPlugin = false
        for (nodeInfo in nodesInfo.values) {
            val plugins = nodeInfo["plugins"] as List<Map<String, Any>>

            for (plugin in plugins) {
                if (plugin["name"] == "opensearch-index-management") {
                    hasIndexStateManagementPlugin = true
                }
                if (plugin["name"] == "opensearch-job-scheduler") {
                    hasJobSchedulerPlugin = true
                }
            }

            if (hasIndexStateManagementPlugin && hasJobSchedulerPlugin) {
                return
            }
        }
        fail("Plugins not installed, ISMPlugin loaded: $hasIndexStateManagementPlugin, JobScheduler loaded: $hasJobSchedulerPlugin")
    }

    @Throws(Exception::class)
    fun `test creating a policy`() {
        val policy = randomPolicy()
        val policyId = OpenSearchTestCase.randomAlphaOfLength(10)
        val createResponse =
            client().makeRequest("PUT", "$POLICY_BASE_URI/$policyId", emptyMap(), policy.toHttpEntity())

        assertEquals("Create policy failed", RestStatus.CREATED, createResponse.restStatus())
        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdSeqNo = responseBody[_SEQ_NO] as Int
        val createdPrimaryTerm = responseBody[_PRIMARY_TERM] as Int
        assertNotEquals("response is missing Id", NO_ID, createdId)
        assertEquals("not same id", policyId, createdId)
        assertEquals("incorrect seqNo", 0, createdSeqNo)
        assertEquals("incorrect primaryTerm", 1, createdPrimaryTerm)

        assertEquals("Incorrect Location header", "$POLICY_BASE_URI/$createdId", createResponse.getHeader("Location"))
    }

    @Throws(Exception::class)
    fun `test creating a policy with no id fails`() {
        try {
            val policy = randomPolicy()
            client().makeRequest("PUT", POLICY_BASE_URI, emptyMap(), policy.toHttpEntity())
            fail("Expected 400 Method BAD_REQUEST response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test creating a policy with a disallowed actions fails`() {
        try {
            // remove read_only from the allowlist
            val allowedActions = ISMActionsParser.instance.parsers.map { it.getActionType() }.toList()
                .filter { actionType -> actionType != ReadOnlyAction.name }
                .joinToString(prefix = "[", postfix = "]") { string -> "\"$string\"" }
            updateClusterSetting(ManagedIndexSettings.ALLOW_LIST.key, allowedActions, escapeValue = false)
            val policy = randomPolicy(states = listOf(randomState(actions = listOf(randomReadOnlyActionConfig()))))
            client().makeRequest("PUT", "$POLICY_BASE_URI/some_id", emptyMap(), policy.toHttpEntity())
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test updating a policy with a disallowed actions fails`() {
        try {
            // remove read_only from the allowlist
            val allowedActions = ISMActionsParser.instance.parsers.map { it.getActionType() }.toList()
                .filter { actionType -> actionType != ReadOnlyAction.name }
                .joinToString(prefix = "[", postfix = "]") { string -> "\"$string\"" }
            updateClusterSetting(ManagedIndexSettings.ALLOW_LIST.key, allowedActions, escapeValue = false)
            // createRandomPolicy currently does not create a random list of actions so it won't accidentally create one with read_only
            val policy = createRandomPolicy()
            // update the policy to have read_only action which is not allowed
            val updatedPolicy = policy.copy(
                defaultState = "some_state",
                states = listOf(randomState(name = "some_state", actions = listOf(randomReadOnlyActionConfig())))
            )
            client().makeRequest(
                "PUT",
                "$POLICY_BASE_URI/${updatedPolicy.id}?refresh=true&if_seq_no=${updatedPolicy.seqNo}&if_primary_term=${updatedPolicy.primaryTerm}",
                emptyMap(), updatedPolicy.toHttpEntity()
            )
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test creating a policy with POST fails`() {
        try {
            val policy = randomPolicy()
            client().makeRequest("POST", "$POLICY_BASE_URI/some_policy", emptyMap(), policy.toHttpEntity())
            fail("Expected 405 Method Not Allowed response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test mappings after policy creation`() {
        createRandomPolicy()

        val response = client().makeRequest("GET", "/$INDEX_MANAGEMENT_INDEX/_mapping")
        val parserMap =
            createParser(XContentType.JSON.xContent(), response.entity.content).map() as Map<String, Map<String, Any>>
        val mappingsMap = parserMap[INDEX_MANAGEMENT_INDEX]!!["mappings"] as Map<String, Any>
        val expected = createParser(
            XContentType.JSON.xContent(),
            javaClass.classLoader.getResource("mappings/opendistro-ism-config.json").readText()
        )
        val expectedMap = expected.map()

        assertEquals("Mappings are different", expectedMap, mappingsMap)
    }

    @Throws(Exception::class)
    fun `test update policy with wrong seq_no and primary_term`() {
        val policy = createRandomPolicy()

        try {
            client().makeRequest(
                "PUT",
                "$POLICY_BASE_URI/${policy.id}?refresh=true&if_seq_no=10251989&if_primary_term=2342",
                emptyMap(), policy.toHttpEntity()
            )
            fail("expected 409 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.CONFLICT, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test update policy with correct seq_no and primary_term`() {
        val policy = createRandomPolicy()
        val updateResponse = client().makeRequest(
            "PUT",
            "$POLICY_BASE_URI/${policy.id}?refresh=true&if_seq_no=${policy.seqNo}&if_primary_term=${policy.primaryTerm}",
            emptyMap(), policy.toHttpEntity()
        )

        assertEquals("Update policy failed", RestStatus.OK, updateResponse.restStatus())
        val responseBody = updateResponse.asMap()
        val updatedId = responseBody[_ID] as String
        val updatedSeqNo = (responseBody[_SEQ_NO] as Int).toLong()
        assertNotEquals("response is missing Id", NO_ID, updatedId)
        assertEquals("not same id", policy.id, updatedId)
        assertTrue("incorrect seqNo", policy.seqNo < updatedSeqNo)
    }

    @Throws(Exception::class)
    fun `test deleting a policy`() {
        val policy = createRandomPolicy()

        val deleteResponse = client().makeRequest("DELETE", "$POLICY_BASE_URI/${policy.id}?refresh=true")
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        val getResponse = client().makeRequest("HEAD", "$POLICY_BASE_URI/${policy.id}")
        assertEquals("Deleted policy still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    @Throws(Exception::class)
    fun `test deleting a policy that doesn't exist`() {
        try {
            client().makeRequest("DELETE", "$POLICY_BASE_URI/foobarbaz")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test getting a policy`() {
        val policy = createRandomPolicy()

        val indexedPolicy = getPolicy(policy.id)

        assertEquals("Indexed and retrieved policy differ", policy, indexedPolicy)
    }

    @Throws(Exception::class)
    fun `test getting a policy that doesn't exist`() {
        try {
            getPolicy(randomAlphaOfLength(20))
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test checking if a policy exists`() {
        val policy = createRandomPolicy()

        val headResponse = client().makeRequest("HEAD", "$POLICY_BASE_URI/${policy.id}")
        assertEquals("Unable to HEAD policy", RestStatus.OK, headResponse.restStatus())
        assertNull("Response contains unexpected body", headResponse.entity)
    }

    @Throws(Exception::class)
    fun `test checking if a non-existent policy exists`() {
        val headResponse = client().makeRequest("HEAD", "$POLICY_BASE_URI/foobarbaz")
        assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
    }

    @Throws(Exception::class)
    fun `test able to fuzzy search policies`() {
        val policy = createRandomPolicy()

        val request = """
            {
                "query": {
                    "query_string": {
                        "default_field": "${Policy.POLICY_TYPE}.${Policy.POLICY_ID_FIELD}",
                        "default_operator": "AND",
                        "query": "*${policy.id.substring(4, 7)}*"
                    }
                }
            }
        """.trimIndent()
        val response = client().makeRequest(
            "POST", "$INDEX_MANAGEMENT_INDEX/_search", emptyMap(),
            StringEntity(request, APPLICATION_JSON)
        )
        assertEquals("Request failed", RestStatus.OK, response.restStatus())
        val searchResponse = SearchResponse.fromXContent(createParser(jsonXContent, response.entity.content))
        assertTrue("Did not find policy using fuzzy search", searchResponse.hits.hits.size == 1)
    }

    fun `test get policies before ism init`() {
        val actualResponse = client().makeRequest(RestRequest.Method.GET.toString(), POLICY_BASE_URI).asMap()
        val expectedResponse = mapOf(
            "policies" to emptyList<Policy>(),
            "total_policies" to 0
        )
        assertEquals(expectedResponse, actualResponse)
    }

    fun `test get policies with actual policy`() {
        val policy = createRandomPolicy()

        val response = client().makeRequest(RestRequest.Method.GET.toString(), POLICY_BASE_URI)

        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            "total_policies" to 1,
            "policies" to listOf(
                mapOf(
                    _SEQ_NO to policy.seqNo,
                    _ID to policy.id,
                    _PRIMARY_TERM to policy.primaryTerm,
                    Policy.POLICY_TYPE to mapOf(
                        "schema_version" to policy.schemaVersion,
                        "policy_id" to policy.id,
                        "last_updated_time" to policy.lastUpdatedTime.toEpochMilli(),
                        "default_state" to policy.defaultState,
                        "ism_template" to null,
                        "description" to policy.description,
                        "error_notification" to policy.errorNotification,
                        "states" to policy.states.map {
                            mapOf(
                                "name" to it.name,
                                "transitions" to it.transitions,
                                "actions" to it.actions
                            )
                        }
                    )
                )
            )
        )

        assertEquals(expectedMessage.toString(), actualMessage.toString())
    }

    fun `test get policies with hyphen`() {
        val randomPolicy = randomPolicy(id = "testing-hyphens-01")
        createPolicy(randomPolicy, policyId = randomPolicy.id, refresh = true)
        val policy = getPolicy(randomPolicy.id)

        val response = client().makeRequest(RestRequest.Method.GET.toString(), "$POLICY_BASE_URI?queryString=*testing-hyphens*")

        val actualMessage = response.asMap()
        val expectedMessage = mapOf(
            "total_policies" to 1,
            "policies" to listOf(
                mapOf(
                    _SEQ_NO to policy.seqNo,
                    _ID to policy.id,
                    _PRIMARY_TERM to policy.primaryTerm,
                    Policy.POLICY_TYPE to mapOf(
                        "schema_version" to policy.schemaVersion,
                        "policy_id" to policy.id,
                        "last_updated_time" to policy.lastUpdatedTime.toEpochMilli(),
                        "default_state" to policy.defaultState,
                        "ism_template" to null,
                        "description" to policy.description,
                        "error_notification" to policy.errorNotification,
                        "states" to policy.states.map {
                            mapOf(
                                "name" to it.name,
                                "transitions" to it.transitions,
                                "actions" to it.actions
                            )
                        }
                    )
                )
            )
        )

        assertEquals(expectedMessage.toString(), actualMessage.toString())
    }
}
