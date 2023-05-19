/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.junit.After
import org.junit.Before
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.indexmanagement.BULK_WRITE_INDEX
import org.opensearch.indexmanagement.CREATE_INDEX
import org.opensearch.indexmanagement.GET_INDEX_MAPPING
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.MANAGED_INDEX
import org.opensearch.indexmanagement.PUT_INDEX_MAPPING
import org.opensearch.indexmanagement.SEARCH_INDEX
import org.opensearch.indexmanagement.SecurityRestTestCase
import org.opensearch.indexmanagement.WRITE_INDEX
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.SecurityRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.AliasAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

@TestLogging("level:DEBUG", reason = "Debug for tests.")
class ExplainSecurityBehaviorIT : SecurityRestTestCase() {
    private val password = "Test123!"
    private val ismUser = "john"

    private var userClient: RestClient? = null

    private val permittedIndiciesPrefix = "permitted-index"
    private val permittedIndicesPattern = "permitted-index*"
    @Before
    fun setupUsersAndRoles() {
        // Init super transform user
        val helpdeskClusterPermissions = listOf(
            ExplainAction.NAME
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
        createRole(HELPDESK_ROLE, helpdeskClusterPermissions, indexPermissions, listOf(permittedIndicesPattern))
        assignRoleToUsers(HELPDESK_ROLE, listOf(ismUser))

        userClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), ismUser, password).setSocketTimeout(60000)
                .build()
    }

    @After
    fun cleanup() {
        userClient?.close()
        deleteUser(ismUser)
        deleteRole(HELPDESK_ROLE)

        deleteIndexByName("${IndexManagementPlugin.INDEX_MANAGEMENT_INDEX}")
    }

    fun `test managed index explain indices permission check`() {

        val notPermittedIndexPrefix = randomAlphaOfLength(10).lowercase(Locale.getDefault())
        val policyId = randomAlphaOfLength(10)

        val permittedindices = mutableListOf<String>()
        val notPermittedindices = mutableListOf<String>()
        for (i in 1..5) {
            createIndex("$notPermittedIndexPrefix-$i", """ "properties": { "field_a": { "type": "long" } }""", client())
            createIndex("$permittedIndiciesPrefix-$i", """ "properties": { "field_a": { "type": "long" } }""", client())
            notPermittedindices += "$notPermittedIndexPrefix-$i"
            permittedindices += "$permittedIndiciesPrefix-$i"
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

            addPolicyToIndex(index = allIndicesJoined, policyId = policy.id, expectedStatus = RestStatus.OK, client = client())

            refreshAllIndices()

            val explainResponse = managedIndicesExplain(notPermittedindices + permittedindices, userClient!!)

            assertEquals(permittedindices.size, explainResponse["total_managed_indices"] as Int)
        } catch (e: ResponseException) {
            fail(e.message)
        } finally {
            deleteIndexByName("$permittedIndiciesPrefix*")
            deleteIndexByName("$notPermittedIndexPrefix*")
        }
    }
}
