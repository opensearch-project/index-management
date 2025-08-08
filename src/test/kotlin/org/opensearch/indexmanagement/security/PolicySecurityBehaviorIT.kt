/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.security

import org.junit.After
import org.junit.Before
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.BULK_WRITE_INDEX
import org.opensearch.indexmanagement.CREATE_INDEX
import org.opensearch.indexmanagement.GET_INDEX_MAPPING
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.MANAGED_INDEX
import org.opensearch.indexmanagement.PUT_INDEX_MAPPING
import org.opensearch.indexmanagement.SEARCH_INDEX
import org.opensearch.indexmanagement.WRITE_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.action.AliasAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale
import kotlin.collections.get

@TestLogging("level:DEBUG", reason = "Debug for tests.")
class PolicySecurityBehaviorIT : SecurityRestTestCase() {
    private val password = "TestpgfhertergGd435AASA123!"

    private val ismUser = "john"
    private var ismUserClient: RestClient? = null

    private val permittedIndicesPrefix = "permitted-index"
    private val permittedIndicesPattern = "permitted-index*"

    @Before
    fun setupUsersAndRoles() {
//        updateClusterSetting(ManagedIndexSettings.JITTER.key, "0.0", false)

        val custerPermissions =
            listOf(
                AddPolicyAction.Companion.NAME,
            )

        val indexPermissions =
            listOf(
                MANAGED_INDEX,
                CREATE_INDEX,
                WRITE_INDEX,
                BULK_WRITE_INDEX,
                GET_INDEX_MAPPING,
                SEARCH_INDEX,
                PUT_INDEX_MAPPING,
            )
        createUser(ismUser, password, listOf(HELPDESK))
        createRole(HELPDESK_ROLE, custerPermissions, indexPermissions, listOf(permittedIndicesPattern))
        assignRoleToUsers(HELPDESK_ROLE, listOf(ismUser))

        ismUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), ismUser, password).setSocketTimeout(60000)
                .build()
    }

    @After
    fun cleanup() {
        // Remove user
        ismUserClient?.close()
        deleteUser(ismUser)
        deleteRole(HELPDESK_ROLE)

        deleteIndexByName("${IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX}")
    }

    fun `test add policy`() {
        val notPermittedIndexPrefix = randomAlphaOfLength(10).lowercase(Locale.getDefault())
        val policyId = randomAlphaOfLength(10)

        val permittedindices = mutableListOf<String>()
        val notPermittedindices = mutableListOf<String>()
        for (i in 1..5) {
            createIndex("$notPermittedIndexPrefix-$i", """ "properties": { "field_a": { "type": "long" } }""", client())
            createIndex("$permittedIndicesPrefix-$i", """ "properties": { "field_a": { "type": "long" } }""", client())
            notPermittedindices += "$notPermittedIndexPrefix-$i"
            permittedindices += "$permittedIndicesPrefix-$i"
        }

        val allIndicesJoined = (notPermittedindices + permittedindices).joinToString(separator = ",")
        try {
            val actions = listOf(IndicesAliasesRequest.AliasActions.add().alias("aaa"))
            val actionConfig = AliasAction(actions = actions, index = 0)
            val states = listOf(State("alias", listOf(actionConfig), listOf()))
            val policy =
                Policy(
                    id = policyId,
                    description = "description",
                    schemaVersion = 1L,
                    lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                    errorNotification = randomErrorNotification(),
                    defaultState = "alias",
                    states = states,
                )
            createPolicy(policy, policy.id, true, client())
            // Call AddPolicyAction as user
            addPolicyToIndex(index = allIndicesJoined, policyId = policy.id, expectedStatus = RestStatus.OK, client = ismUserClient!!)

            refreshAllIndices()

            val explainResponseAsMap = managedIndexExplainAllAsMap(client())
            assertEquals(5, explainResponseAsMap["total_managed_indices"] as Int)
        } catch (e: ResponseException) {
            logger.error(e.message, e)
        } finally {
            deleteIndexByName("$permittedIndicesPrefix*")
            deleteIndexByName("$notPermittedIndexPrefix*")
        }
    }
}
