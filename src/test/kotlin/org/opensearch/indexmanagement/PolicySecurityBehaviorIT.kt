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

import org.junit.After
import org.junit.Before
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.action.AliasAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

@TestLogging("level:DEBUG", reason = "Debug for tests.")
class PolicySecurityBehaviorIT : SecurityRestTestCase() {
    private val password = "Test123!"

    private val ismUser = "john"
    private var ismUserClient: RestClient? = null

    private val permittedIndicesPrefix = "permitted-index"
    private val permittedIndicesPattern = "permitted-index*"
    @Before
    fun setupUsersAndRoles() {
//        updateClusterSetting(ManagedIndexSettings.JITTER.key, "0.0", false)

        val custerPermissions = listOf(
            AddPolicyAction.NAME
        )

        val indexPermissions = listOf(
            MANAGED_INDEX,
            CREATE_INDEX,
            WRITE_INDEX,
            BULK_WRITE_INDEX,
            GET_INDEX_MAPPING,
            SEARCH_INDEX,
            PUT_INDEX_MAPPING
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

        deleteIndexByName("$INDEX_MANAGEMENT_INDEX")
    }

    fun `test add policy`() {

        val notPermittedIndexPrefix = OpenSearchTestCase.randomAlphaOfLength(10).lowercase(Locale.getDefault())
        val policyId = OpenSearchTestCase.randomAlphaOfLength(10)

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
            val policy = Policy(
                id = policyId,
                description = "description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = "alias",
                states = states
            )
            createPolicy(policy, policy.id, true, client())
            // Call AddPolicyAction as user
            addPolicyToIndex(index = allIndicesJoined, policyId = policy.id, expectedStatus = RestStatus.OK, client = ismUserClient!!)

            refreshAllIndices()

            val searchResponse = responseAsMap(client().makeRequest(RestRequest.Method.GET.toString(), "$INDEX_MANAGEMENT_INDEX/_search?size=1000"))
            val numOfHits = ((searchResponse["hits"] as Map<*, *>)["total"] as Map<*, *>)["value"] as Int
            // 1 Policy document + 5 ManagedIndex docs
            assertEquals(1 + 5, numOfHits)
        } catch (e: ResponseException) {
            logger.error(e.message, e)
        } finally {
            deleteIndexByName("$permittedIndicesPrefix*")
            deleteIndexByName("$notPermittedIndexPrefix*")
        }
    }
}
