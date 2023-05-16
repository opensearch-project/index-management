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
    private var superUserClient: RestClient? = null

    private val testUser = "testUser"
    private val testRole = "test_role"
    var testUserClient: RestClient? = null

    private val PERMITTED_INDICES_PREFIX = "permitted-index"
    private val PERMITTED_INDICES_PATTERN = "permitted-index*"
    @Before
    fun setupUsersAndRoles() {
//        updateClusterSetting(ManagedIndexSettings.JITTER.key, "0.0", false)

        // Init super transform user
        val helpdeskClusterPermissions = listOf(
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
        // In this test suite case john is a "super-user" which has all relevant privileges
        createUser(ismUser, password, listOf(HELPDESK))
        createRole(HELPDESK_ROLE, helpdeskClusterPermissions, indexPermissions, listOf(PERMITTED_INDICES_PATTERN))
        assignRoleToUsers(HELPDESK_ROLE, listOf(ismUser))

        superUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), ismUser, password).setSocketTimeout(60000)
                .build()
    }

    @After
    fun cleanup() {
        // Remove super user
        superUserClient?.close()
        deleteUser(ismUser)
        deleteRole(HELPDESK_ROLE)
        // Remove test user
        testUserClient?.close()
        deleteUser(testUser)
        deleteRole(testRole)

        deleteIndexByName(".opendistro-ism-config")
    }

    fun `test add policy`() {

        val notPermittedIndexPrefix = OpenSearchTestCase.randomAlphaOfLength(10).lowercase(Locale.getDefault())
        val policyId = OpenSearchTestCase.randomAlphaOfLength(10)

        val permittedindices = mutableListOf<String>()
        val notPermittedindices = mutableListOf<String>()
        for (i in 1..5) {
            createIndex("$notPermittedIndexPrefix-$i", """ "properties": { "field_a": { "type": "long" } }""", client())
            createIndex("$PERMITTED_INDICES_PREFIX-$i", """ "properties": { "field_a": { "type": "long" } }""", client())
            notPermittedindices += "$notPermittedIndexPrefix-$i"
            permittedindices += "$PERMITTED_INDICES_PREFIX-$i"
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

            addPolicyToIndex(index = allIndicesJoined, policyId = policy.id, expectedStatus = RestStatus.OK, client = superUserClient!!)

            val searchResponse = responseAsMap(client().makeRequest(RestRequest.Method.GET.toString(), "$INDEX_MANAGEMENT_INDEX/_search?size=1000"))

            val numOfHits = ((searchResponse["hits"] as Map<*, *>)["total"] as Map<*, *>)["value"] as Int
            assertEquals(1 + 5, numOfHits)
        } catch (e: ResponseException) {
            logger.error(e.message, e)
        } finally {
            deleteIndexByName("$PERMITTED_INDICES_PREFIX*")
            deleteIndexByName("$notPermittedIndexPrefix*")
        }
    }

    private fun createTestUserWithRole(clusterPermissions: List<String>, indexPermissions: List<String>) {
        val testBackendRole = testRole + "_backend"
        // In this test suite case john is a "super-user" which has all relevant privileges
        createUser(testUser, password, listOf(testBackendRole))
        createRole(testRole, clusterPermissions, indexPermissions, listOf(AIRLINE_INDEX_PATTERN))
        assignRoleToUsers(testRole, listOf(testUser))
    }
}
