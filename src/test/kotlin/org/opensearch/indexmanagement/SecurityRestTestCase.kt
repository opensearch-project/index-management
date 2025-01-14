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

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.HttpHeaders
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.http.message.BasicHeader
import org.opensearch.client.Request
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.Strings
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestExplainAction
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.toJsonString
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_REPLICAS
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
import org.opensearch.indexmanagement.rollup.RollupRestTestCase
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.toJsonString
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.transform.TransformRestTestCase
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.toJsonString
import org.opensearch.rest.RestRequest
import org.opensearch.test.OpenSearchTestCase
import java.util.Locale

abstract class SecurityRestTestCase : IndexManagementRestTestCase() {
    private object RollupRestTestCaseSecurityExtension : RollupRestTestCase() {
        fun createRollupExt(
            rollup: Rollup,
            rollupId: String,
            refresh: Boolean,
            client: RestClient,
        ) = super.createRollup(rollup, rollupId, refresh, client)

        fun getRollupMetadataExt(
            metadataId: String,
            refresh: Boolean = true,
            header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
        ) = super.getRollupMetadata(metadataId, refresh, header)

        fun getRollupExt(
            rollupId: String,
            header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
        ) = super.getRollup(rollupId, header)

        fun putDateDocumentInSourceIndexExt(rollup: Rollup) = super.putDateDocumentInSourceIndex(rollup)

        fun createRollupMappingStringExt(rollup: Rollup): String = super.createRollupMappingString(rollup)
    }

    private object IndexStateManagementRestTestCaseExt : IndexStateManagementRestTestCase() {
        fun createPolicyExt(
            policy: Policy,
            policyId: String = OpenSearchTestCase.randomAlphaOfLength(10),
            refresh: Boolean = true,
            client: RestClient?,
        ) = super.createPolicy(policy, policyId, refresh, client)

        fun createPolicyJsonExt(
            policyString: String,
            policyId: String,
            refresh: Boolean = true,
            client: RestClient,
        ) = super.createPolicyJson(policyString, policyId, refresh, client)

        fun updateManagedIndexConfigStartTimeExt(update: ManagedIndexConfig, desiredStartTimeMillis: Long? = null, retryOnConflict: Int = 0) = super.updateManagedIndexConfigStartTime(update, desiredStartTimeMillis, retryOnConflict)

        fun getExistingManagedIndexConfigExt(index: String) = super.getExistingManagedIndexConfig(index)

        fun createIndexExt(
            index: String = randomAlphaOfLength(10).lowercase(Locale.ROOT),
            policyID: String? = randomAlphaOfLength(10),
            alias: String? = null,
            replicas: String? = null,
            shards: String? = null,
            mapping: String = "",
            settings: Settings? = null,
        ): Pair<String, String?> = super.createIndex(index, policyID, alias, replicas, shards, mapping, settings)

        fun getExplainManagedIndexMetaDataExt(indexName: String, userClient: RestClient? = null): ManagedIndexMetaData = super.getExplainManagedIndexMetaData(indexName, userClient)
    }

    private object TransformRestTestCaseExt : TransformRestTestCase() {
        fun createTransformExt(
            transform: Transform,
            transformId: String = randomAlphaOfLength(10),
            refresh: Boolean = true,
            client: RestClient? = null,
        ) = super.createTransform(transform, transformId, refresh, client)

        fun getTransformExt(
            transformId: String,
            header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
            userClient: RestClient? = null,
        ) = super.getTransform(transformId, header, userClient)

        fun getTransformMetadataExt(metadataId: String) = super.getTransformMetadata(metadataId)
    }

    protected fun updateClusterSetting(key: String, value: String, escapeValue: Boolean = true) {
        val formattedValue = if (escapeValue) "\"$value\"" else value
        val request =
            """
            {
                "persistent": {
                    "$key": $formattedValue
                }
            }
            """.trimIndent()
        val res =
            client().makeRequest(
                "PUT", "_cluster/settings", emptyMap(),
                StringEntity(request, ContentType.APPLICATION_JSON),
            )
        assertEquals("Request failed", RestStatus.OK, res.restStatus())
    }

    protected fun createRollup(
        rollup: Rollup,
        client: RestClient,
    ): Rollup = RollupRestTestCaseSecurityExtension.createRollupExt(rollup, rollup.id, true, client)

    protected fun createRollupAndCheckStatus(
        rollup: Rollup,
        expectedStatus: RestStatus,
        client: RestClient,
    ): Response {
        val request = Request("PUT", "${IndexManagementPlugin.ROLLUP_JOBS_BASE_URI}/${rollup.id}?refresh=true")
        request.setJsonEntity(rollup.toJsonString())
        return executeRequest(request, expectedStatus, client)
    }

    protected fun getRollupMetadata(
        metadataId: String,
        refresh: Boolean = true,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): RollupMetadata = RollupRestTestCaseSecurityExtension.getRollupMetadataExt(metadataId, refresh, header)

    protected fun getRollup(
        rollupId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): Rollup = RollupRestTestCaseSecurityExtension.getRollupExt(rollupId, header)

    protected fun deleteRollup(rollupId: String, client: RestClient, expectedStatus: RestStatus) {
        val request = Request(RestRequest.Method.DELETE.name, "${IndexManagementPlugin.ROLLUP_JOBS_BASE_URI}/$rollupId")
        executeRequest(request, expectedStatus, client)
    }

    protected fun createRollupSourceIndex(rollup: Rollup, settings: Settings = Settings.EMPTY) {
        createIndex(rollup.sourceIndex, settings, createRollupMappingString(rollup))
    }

    protected fun putDateDocumentInSourceIndex(rollup: Rollup) {
        RollupRestTestCaseSecurityExtension.putDateDocumentInSourceIndexExt(rollup)
    }

    private fun createRollupMappingString(rollup: Rollup): String = RollupRestTestCaseSecurityExtension.createRollupMappingStringExt(rollup)

    protected fun updateManagedIndexConfigStartTime(update: ManagedIndexConfig, desiredStartTimeMillis: Long? = null, retryOnConflict: Int = 0) = IndexStateManagementRestTestCaseExt.updateManagedIndexConfigStartTimeExt(update, desiredStartTimeMillis, retryOnConflict)

    protected fun createPolicy(
        policy: Policy,
        policyId: String = OpenSearchTestCase.randomAlphaOfLength(10),
        refresh: Boolean = true,
        client: RestClient?,
    ): Policy = IndexStateManagementRestTestCaseExt.createPolicyExt(policy, policyId, refresh, client)

    protected fun managedIndexExplainAllAsMap(
        client: RestClient?,
    ): Map<*, *> {
        val request = Request("GET", "${RestExplainAction.EXPLAIN_BASE_URI}")
        return entityAsMap(executeRequest(request, RestStatus.OK, client!!))
    }

    protected fun getExistingManagedIndexConfig(index: String) = IndexStateManagementRestTestCaseExt.getExistingManagedIndexConfigExt(index)

    protected fun createPolicyJson(
        policyString: String,
        policyId: String,
        refresh: Boolean = true,
        client: RestClient,
    ): Response = IndexStateManagementRestTestCaseExt.createPolicyJsonExt(policyString, policyId, refresh, client)

    protected fun deletePolicy(policyId: String, client: RestClient, expectedStatus: RestStatus) {
        val request = Request("DELETE", "${IndexManagementPlugin.POLICY_BASE_URI}/$policyId")
        executeRequest(request, expectedStatus, client)
    }

    protected fun addPolicyToIndex(index: String, policyId: String, expectedStatus: RestStatus, client: RestClient) {
        val body =
            """
            {
              "policy_id": "$policyId"
            }
            """.trimIndent()
        val request = Request("POST", "/_opendistro/_ism/add/$index")
        request.setJsonEntity(body)
        executeRequest(request, expectedStatus, client)
    }

    protected fun getExplainManagedIndexMetaData(indexName: String, userClient: RestClient? = null): ManagedIndexMetaData = IndexStateManagementRestTestCaseExt.getExplainManagedIndexMetaDataExt(indexName, userClient)

    protected fun createIndex(indexName: String, sourceIndexMappingString: String?, client: RestClient) {
        val waitForActiveShards = if (isMultiNode) "all" else "1"
        val builtSettings =
            Settings.builder().let {
                it.putNull(ManagedIndexSettings.ROLLOVER_ALIAS.key)
                it.put(INDEX_NUMBER_OF_REPLICAS, "1")
                it.put(INDEX_NUMBER_OF_SHARDS, "1")
                it.put("index.write.wait_for_active_shards", waitForActiveShards)
                it
            }.build()

        val request = Request("PUT", "/$indexName")
        var entity = "{\"settings\": " + Strings.toString(XContentType.JSON, builtSettings)
        if (sourceIndexMappingString != null) {
            entity = "$entity,\"mappings\" : {$sourceIndexMappingString}"
        }
        entity = "$entity}"
        request.setJsonEntity(entity)
        client.performRequest(request)
    }

    fun createIndex(
        index: String = randomAlphaOfLength(10).lowercase(Locale.ROOT),
        policyID: String? = randomAlphaOfLength(10),
        alias: String? = null,
        replicas: String? = null,
        shards: String? = null,
        mapping: String = "",
        settings: Settings? = null,
    ): Pair<String, String?> = IndexStateManagementRestTestCaseExt.createIndexExt(index, policyID, alias, replicas, shards, mapping, settings)

    protected fun checkPolicies(
        userClient: RestClient,
        user: String,
        expectedNumberOfPolicies: Int?,
        expectedStatus: RestStatus = RestStatus.OK,
    ): Response? {
        val response = executeRequest(request = Request(RestRequest.Method.GET.name, IndexManagementPlugin.POLICY_BASE_URI), expectedStatus, userClient)
        assertEquals(
            "User $user not able to see all policies", expectedNumberOfPolicies, response.asMap()["total_policies"],
        )
        return response
    }

    protected fun checkIndexAccess(index: String, userClient: RestClient, expectedStatus: RestStatus): Response {
        val request = Request(RestRequest.Method.GET.name, "$index/_search")
        return executeRequest(request, expectedStatus, userClient)
    }

    protected fun checkPolicyGet(policyId: String, userClient: RestClient, expectedStatus: RestStatus): Response {
        val request = Request(RestRequest.Method.GET.name, IndexManagementPlugin.POLICY_BASE_URI + "/$policyId")
        return executeRequest(request, expectedStatus, userClient)
    }

    protected fun checkRollupExplain(rollupId: String, userClient: RestClient, expectedStatus: RestStatus) {
        val request = Request(RestRequest.Method.GET.name, "${IndexManagementPlugin.ROLLUP_JOBS_BASE_URI}/$rollupId/_explain")
        executeRequest(request, expectedStatus, userClient)
    }

    protected fun checkRollupGet(rollupId: String, userClient: RestClient, expectedStatus: RestStatus) {
        val request = Request(RestRequest.Method.GET.name, "${IndexManagementPlugin.ROLLUP_JOBS_BASE_URI}/$rollupId")
        executeRequest(request, expectedStatus, userClient)
    }

    protected fun createTransform(
        transform: Transform,
        transformId: String = randomAlphaOfLength(10),
        refresh: Boolean = true,
        client: RestClient? = null,
    ) = TransformRestTestCaseExt.createTransformExt(transform, transformId, refresh, client)

    protected fun createTransformAndCheckStatus(
        transform: Transform,
        expectedStatus: RestStatus,
        client: RestClient,
    ): Response {
        val request = Request(RestRequest.Method.PUT.name, "${IndexManagementPlugin.TRANSFORM_BASE_URI}/${transform.id}?refresh=true")
        request.setJsonEntity(transform.toJsonString())
        return executeRequest(request, expectedStatus, client)
    }

    protected fun createPolicyAndCheckStatus(
        policy: Policy,
        expectedStatus: RestStatus,
        client: RestClient,
    ): Response {
        val request = Request("PUT", "${IndexManagementPlugin.POLICY_BASE_URI}/${policy.id}?refresh=true")
        request.setJsonEntity(policy.toJsonString())
        return executeRequest(request, expectedStatus, client)
    }

    protected fun getTransform(
        transformId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
        client: RestClient,
    ) = TransformRestTestCaseExt.getTransformExt(transformId, header, client)

    protected fun getTransformMetadata(metadataId: String) = TransformRestTestCaseExt.getTransformMetadataExt(metadataId)

    protected fun checkTransformExplain(transformId: String, userClient: RestClient, expectedStatus: RestStatus): Response {
        val request = Request(RestRequest.Method.GET.name, IndexManagementPlugin.TRANSFORM_BASE_URI + "/$transformId/_explain")
        return executeRequest(request, expectedStatus, userClient)
    }

    protected fun checkTransformGet(transformId: String, userClient: RestClient, expectedStatus: RestStatus): Response {
        val request = Request(RestRequest.Method.GET.name, IndexManagementPlugin.TRANSFORM_BASE_URI + "/$transformId")
        return executeRequest(request, expectedStatus, userClient)
    }

    protected fun stopTransform(transformId: String, client: RestClient, expectedStatus: RestStatus) {
        val request = Request(RestRequest.Method.POST.name, "${IndexManagementPlugin.TRANSFORM_BASE_URI}/$transformId/_stop")
        executeRequest(request, expectedStatus, client)
    }

    protected fun deleteTransform(transformId: String, client: RestClient, expectedStatus: RestStatus) {
        val request = Request("DELETE", "${IndexManagementPlugin.TRANSFORM_BASE_URI}/$transformId")
        executeRequest(request, expectedStatus, client)
    }

    protected fun checkTransformPreview(userClient: RestClient, expectedStatus: RestStatus): Response {
        val request = Request(RestRequest.Method.GET.name, "${IndexManagementPlugin.TRANSFORM_BASE_URI}/_preview")
        return executeRequest(request, expectedStatus, userClient)
    }

    protected fun executeRequest(
        request: Request,
        expectedRestStatus: RestStatus? = null,
        client: RestClient,
    ): Response {
        val response =
            try {
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
        val json =
            """
            {
                "password": "$pwd",
                "backend_roles": [$backendRolesStr],
                "attributes":{}
            }
            """.trimIndent()
        val request = Request(RestRequest.Method.PUT.name, "_plugins/_security/api/internalusers/$name")
        request.setJsonEntity(json)

        executeRequest(request, RestStatus.CREATED, client())
    }

    protected fun createUserWithCustomRole(
        user: String,
        password: String,
        role: String,
        clusterPermissions: List<String> = emptyList(),
        indexPermissions: List<String> = emptyList(),
        backendRoles: List<String> = emptyList(),
        indexPatterns: List<String> = emptyList(),
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
        val response =
            try {
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
        val entity =
            """
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
        executeRequest(request, RestStatus.CREATED, client())
    }

    protected fun assignRoleToUsers(role: String, users: List<String>) {
        val request = Request("PUT", "/_plugins/_security/api/rolesmapping/$role")
        val usersStr = users.joinToString { "\"$it\"" }
        val entity =
            """
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
        val setting =
            """
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
        executeRequest(request = Request(RestRequest.Method.DELETE.name, "/_plugins/_security/api/internalusers/$name"), client = client())
    }

    protected fun deleteRole(name: String) {
        executeRequest(request = Request(RestRequest.Method.DELETE.name, "/_plugins/_security/api/roles/$name"), client = client())
    }

    protected fun deleteIndexByName(index: String) {
        executeRequest(request = Request(RestRequest.Method.DELETE.name, "/$index"), client = adminClient())
    }

    protected fun validateSourceIndex(indexName: String) {
        if (!indexExists(indexName)) {
            generateNYCTaxiData(indexName)
            assertIndexExists(indexName)
        }
    }

    protected fun createReplicaCountTestPolicyRequest(priority: Int, indexPattern: String?): String = """
            {
                "policy": {
                    "description": "test policy",
                    "default_state": "start",
                    "states": [
                        {
                            "name": "start",
                            "actions": [
                                {
                                    "replica_count": {
                                        "number_of_replicas": 5
                                    }
                                }
                            ],
                            "transitions": []
                        }
                    ],
                    "ism_template": {
                        "index_patterns": ["$indexPattern"],
                        "priority": $priority
                    }
                }
            }
    """.trimIndent()

    companion object {
        const val AIRLINE_POLICY = "airline-policy"
        const val AVAILABILITY_POLICY = "availability-policy"

        const val AIRLINE_INDEX = "airline-1"
        const val AVAILABILITY_INDEX = "availability-1"

        const val PHONE_OPERATOR = "phone_operator"
        const val HELPDESK = "helpdesk_staff"
        const val HELPDESK_ROLE = "helpdesk_role"
        const val PHONE_OPERATOR_ROLE = "phone_operator_role"

        const val AIRLINE_INDEX_PATTERN = "airline-*"
        const val AVAILABILITY_INDEX_PATTERN = "availability-*"
    }
}
