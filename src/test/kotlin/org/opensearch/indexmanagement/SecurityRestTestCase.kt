/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement

import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.client.Request
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.common.Strings
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestExplainAction
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.toJsonString
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_REPLICAS
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.toJsonString
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase

abstract class SecurityRestTestCase : IndexManagementRestTestCase() {

    protected fun createRollup(
        rollup: Rollup,
        rollupUserClient: RestClient,
    ) {
        val request = Request("PUT", "${IndexManagementPlugin.ROLLUP_JOBS_BASE_URI}/${rollup.id}")
        request.entity = rollup.toHttpEntity()
        val response = executeRequest(request, RestStatus.CREATED, rollupUserClient)

        val responseBody = response.asMap()
        val createdId = responseBody["_id"] as String
        assertNotEquals("Response is missing Id", NO_ID, createdId)
        assertEquals("Not same id", rollup.id, createdId)
        assertEquals(
            "Incorrect Location header",
            "${IndexManagementPlugin.ROLLUP_JOBS_BASE_URI}/$createdId",
            response.getHeader("Location")
        )
    }

    protected fun checkPolicies(
        userClient: RestClient,
        user: String,
        expectedNumberOfPolicies: Int?,
        expectedStatus: RestStatus = RestStatus.OK
    ): Response? {
        val response = executeRequest(request = Request(RestRequest.Method.GET.name, IndexManagementPlugin.POLICY_BASE_URI), expectedStatus, userClient)
        assertEquals(
            "User $user not able to see all policies", expectedNumberOfPolicies, response.asMap()["total_policies"]
        )
        return response
    }

    protected fun checkIndexAccess(index: String, userClient: RestClient, expectedStatus: RestStatus): Response {
        val request = Request(RestRequest.Method.GET.name, "$index/_search")
        return executeRequest(request, expectedStatus, userClient)
    }

    protected fun checkPolicyAccess(policyId: String, userClient: RestClient, expectedStatus: RestStatus): Response {
        val request = Request(RestRequest.Method.GET.name, IndexManagementPlugin.POLICY_BASE_URI + "/$policyId")
        return executeRequest(request, expectedStatus, userClient)
    }

    protected fun checkRollupAccess(rollupId: String, userClient: RestClient, expectedStatus: RestStatus) {
        val request = Request(RestRequest.Method.GET.name, "${IndexManagementPlugin.ROLLUP_JOBS_BASE_URI}/$rollupId/_explain")
        executeRequest(request, expectedStatus, userClient)
    }

    protected fun createPolicy(
        policy: Policy,
        policyId: String = OpenSearchTestCase.randomAlphaOfLength(10),
        refresh: Boolean = true,
        client: RestClient,
    ): Policy {
        val response = createPolicyJson(policy.toJsonString(), policyId, refresh, client)

        val policyJson = JsonXContent.jsonXContent
            .createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                response.entity.content
            ).map()
        val createdId = policyJson["_id"] as String
        assertEquals("policy ids are not the same", policyId, createdId)
        return policy.copy(
            id = createdId,
            seqNo = (policyJson["_seq_no"] as Int).toLong(),
            primaryTerm = (policyJson["_primary_term"] as Int).toLong()
        )
    }

    @Suppress("LoopWithTooManyJumpStatements")
    protected fun getExplainManagedIndexMetaData(indexName: String, userClient: RestClient): ManagedIndexMetaData {
        if (indexName.contains("*") || indexName.contains(",")) {
            throw IllegalArgumentException("This method is only for a single concrete index")
        }

        val response = userClient.makeRequest(RestRequest.Method.GET.toString(), "${RestExplainAction.EXPLAIN_BASE_URI}/$indexName")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        lateinit var metadata: ManagedIndexMetaData
        val xcp = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val cn = xcp.currentName()
            xcp.nextToken()
            if (cn == "total_managed_indices") continue

            metadata = ManagedIndexMetaData.parse(xcp)
            break // bypass roles field
        }
        return metadata
    }

    protected fun createPolicyJson(
        policyString: String,
        policyId: String,
        refresh: Boolean = true,
        client: RestClient
    ): Response {
        val request = Request(RestRequest.Method.PUT.name, "${IndexManagementPlugin.POLICY_BASE_URI}/$policyId?refresh=$refresh")
        request.setJsonEntity(policyString)
        return executeRequest(request, RestStatus.CREATED, client)
    }

    protected fun createIndex(indexName: String, sourceIndexMappingString: String?, client: RestClient) {
        val waitForActiveShards = if (isMultiNode) "all" else "1"
        val builtSettings = Settings.builder().let {
            it.putNull(ManagedIndexSettings.ROLLOVER_ALIAS.key)
            it.put(INDEX_NUMBER_OF_REPLICAS, "1")
            it.put(INDEX_NUMBER_OF_SHARDS, "1")
            it.put("index.write.wait_for_active_shards", waitForActiveShards)
            it
        }.build()

        val request = Request("PUT", "/$indexName")
        var entity = "{\"settings\": " + Strings.toString(builtSettings)
        if (sourceIndexMappingString != null) {
            entity = "$entity,\"mappings\" : {$sourceIndexMappingString}"
        }
        entity = "$entity}"
        request.setJsonEntity(entity)
        client.performRequest(request)
    }

    protected fun executeRequest(
        request: Request,
        expectedRestStatus: RestStatus? = null,
        client: RestClient
    ): Response {
        val response = try {
            client.performRequest(request)
        } catch (exception: ResponseException) {
            exception.response
        }
        if (expectedRestStatus != null) {
            assertEquals(expectedRestStatus.status, response.statusLine.statusCode)
        }
        return response
    }

    protected fun createUser(name: String, pwd: String = "Test123!", backendRoles: List<String> = listOf()) {
        val backendRolesStr = backendRoles.joinToString { "\"$it\"" }
        val json = """
            {
                "password": "$pwd",
                "backend_roles": [$backendRolesStr],
                "attributes":{}
            }
        """.trimIndent()
        val request = Request(RestRequest.Method.PUT.name, "_plugins/_security/api/internalusers/$name")
        request.setJsonEntity(json)

        executeRequest(request, null, client())
    }

    protected fun createUserWithCustomRole(
        user: String,
        password: String,
        role: String,
        clusterPermissions: List<String> = emptyList(),
        indexPermissions: List<String> = emptyList(),
        backendRoles: List<String> = emptyList(),
        indexPatterns: List<String> = emptyList()
    ) {
        createUser(user, password, backendRoles)
        createRole(role, clusterPermissions, indexPermissions, indexPatterns)
        assignRoleToUsers(role, listOf(user))
    }

    protected fun createRole(
        name: String,
        clusterPermissions: List<String>,
        indexPermissions: List<String>,
        indexPatterns: List<String>,
    ) {
        val response = try {
            client().performRequest(Request("GET", "/_plugins/_security/api/roles/$name"))
        } catch (ex: ResponseException) {
            ex.response
        }
        // If role already exist, do nothing
        if (response.statusLine.statusCode == RestStatus.OK.status) {
            return
        }

        val request = Request("PUT", "/_plugins/_security/api/roles/$name")
        val indexPatternsStr = indexPatterns.joinToString { "\"$it\"" }
        val clusterPermissionsStr = clusterPermissions.joinToString { "\"$it\"" }
        val indexPermissionsStr = indexPermissions.joinToString { "\"$it\"" }
        val entity = """
            {
                "cluster_permissions": [$clusterPermissionsStr],
                "index_permissions": [
                {
                    "fls": [],
                    "masked_fields": [],
                    "allowed_actions": [$indexPermissionsStr],
                    "index_patterns": [$indexPatternsStr]
                }
                ],
                "tenant_permissions": []
            }
        """.trimIndent()

        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    protected fun assignRoleToUsers(role: String, users: List<String>) {
        val request = Request("PUT", "/_plugins/_security/api/rolesmapping/$role")
        val usersStr = users.joinToString { "\"$it\"" }
        var entity = """
            {
                "backend_roles": [],
                "hosts": [],
                "users": [$usersStr]
            }
        """.trimIndent()
        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    protected fun setFilterByBackendRole(filter: Boolean) {
        val setting = """
            {
                "persistent": {
                    "plugins.index_management.filter_by_backend_roles": "$filter"
                } 
            }
        """.trimIndent()
        val request = Request(RestRequest.Method.PUT.name, "_cluster/settings")
        request.setJsonEntity(setting)
        executeRequest(request, RestStatus.OK, client())
    }

    protected fun deleteUser(name: String) {
        client().makeRequest(RestRequest.Method.DELETE.name, "/_plugins/_security/api/internalusers/$name")
    }

    protected fun deleteRole(name: String) {
        client().makeRequest(RestRequest.Method.DELETE.name, "/_plugins/_security/api/roles/$name")
    }

    private fun Rollup.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), ContentType.APPLICATION_JSON)
}
