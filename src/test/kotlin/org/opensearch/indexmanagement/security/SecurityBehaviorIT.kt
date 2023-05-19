/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.security

import org.junit.After
import org.junit.Before
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.indexmanagement.BULK_WRITE_INDEX
import org.opensearch.indexmanagement.CREATE_INDEX
import org.opensearch.indexmanagement.EXPLAIN_INDEX
import org.opensearch.indexmanagement.GET_INDEX_MAPPING
import org.opensearch.indexmanagement.GET_POLICIES
import org.opensearch.indexmanagement.GET_POLICY
import org.opensearch.indexmanagement.MANAGED_INDEX
import org.opensearch.indexmanagement.PUT_INDEX_MAPPING
import org.opensearch.indexmanagement.SEARCH_INDEX
import org.opensearch.indexmanagement.SecurityRestTestCase
import org.opensearch.indexmanagement.WRITE_INDEX
import org.opensearch.indexmanagement.WRITE_POLICY
import org.opensearch.indexmanagement.SecurityRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
class SecurityBehaviorIT : SecurityRestTestCase() {
    private val password = "TestpgfhertergGd435AASA123!"

    private val john = "john"
    private var johnClient: RestClient? = null

    @Before
    fun setupUsersAndRoles() {
        updateClusterSetting(ManagedIndexSettings.JITTER.key, "0.0", false)

        val helpdeskClusterPermissions = listOf(
            WRITE_POLICY,
            GET_POLICY,
            GET_POLICIES,
            EXPLAIN_INDEX
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
        createUser(john, password, listOf(HELPDESK))
        createRole(HELPDESK_ROLE, helpdeskClusterPermissions, indexPermissions, listOf(AIRLINE_INDEX_PATTERN))
        assignRoleToUsers(HELPDESK_ROLE, listOf(john))

        johnClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), john, password).setSocketTimeout(60000)
                .setConnectionRequestTimeout(180000)
                .build()
    }

    @After
    fun cleanup() {
        johnClient?.close()
        deleteUser(john)
        deleteRole(HELPDESK_ROLE)
        deleteIndexByName(".opendistro-ism-config")
    }

    fun `test policy access based on the backend roles`() {
        val jill = "jill"
        val jane = "jane"
        // Create jill without assigning a role
        createUser(jill, password, listOf(HELPDESK))

        val phoneOperatorClusterPermissions = listOf(
            EXPLAIN_INDEX,
            GET_POLICY,
            WRITE_POLICY,
            GET_POLICIES
        )

        val indexPermissions = listOf(
            MANAGED_INDEX,
            CREATE_INDEX,
            GET_INDEX_MAPPING,
            SEARCH_INDEX,
            PUT_INDEX_MAPPING
        )
        // Jane is phone operator; Phone operators can search availability indexes
        createUserWithCustomRole(
            jane,
            password,
            PHONE_OPERATOR_ROLE,
            phoneOperatorClusterPermissions,
            indexPermissions,
            listOf(PHONE_OPERATOR),
            listOf(AVAILABILITY_INDEX_PATTERN)
        )

        val jillClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), jill, password).setSocketTimeout(60000)
                .setConnectionRequestTimeout(180000)
                .build()
        val janeClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), jane, password).setSocketTimeout(60000)
                .setConnectionRequestTimeout(180000)
                .build()

        setFilterByBackendRole(false)

        try {
            val airlinePolicyJson = createReplicaCountTestPolicyRequest(10, AIRLINE_INDEX_PATTERN)
            createPolicyJson(airlinePolicyJson, AIRLINE_POLICY, true, johnClient!!)

            val availabilityPolicyJson = createReplicaCountTestPolicyRequest(15, AVAILABILITY_INDEX_PATTERN)
            createPolicyJson(availabilityPolicyJson, AVAILABILITY_POLICY, true, janeClient!!)

            checkPolicies(johnClient!!, john, 2)
            // Confirm that jill can't see the policy because of missing privilege
            checkPolicies(jillClient, jill, null, RestStatus.FORBIDDEN)
            checkPolicies(janeClient, jane, 2)

            // Assign jill to airline role
            assignRoleToUsers(HELPDESK_ROLE, listOf(john, jill))
            checkPolicies(jillClient, jill, 2)
            // Confirm that jill can see the policy
            checkPolicyGet(AIRLINE_POLICY, jillClient, RestStatus.OK)

            // Create two index - one for helpdesk service and one for phone operators
            client().makeRequest("PUT", "/$AIRLINE_INDEX")
            client().makeRequest("PUT", "/$AVAILABILITY_INDEX")

            // Change the start time so that the policy will be initialized.
            val airlineIndexConfig = getExistingManagedIndexConfig(AIRLINE_INDEX)
            updateManagedIndexConfigStartTime(airlineIndexConfig)

            waitFor {
                var airlineIndex = getExplainManagedIndexMetaData(AIRLINE_INDEX, johnClient!!)
                assertEquals(AIRLINE_POLICY, airlineIndex.policyID)
                assertEquals(AIRLINE_INDEX, airlineIndex.index)

                airlineIndex = getExplainManagedIndexMetaData(AIRLINE_INDEX, janeClient)
                assertEquals(AIRLINE_POLICY, airlineIndex.policyID)
                assertEquals(AIRLINE_INDEX, airlineIndex.index)
            }

            val availabilityIndexConfig = getExistingManagedIndexConfig(AVAILABILITY_INDEX)
            updateManagedIndexConfigStartTime(availabilityIndexConfig)

            waitFor {
                val availabilityIndex = getExplainManagedIndexMetaData(AVAILABILITY_INDEX, janeClient)
                assertEquals(AVAILABILITY_POLICY, availabilityIndex.policyID)
                assertEquals(AVAILABILITY_INDEX, availabilityIndex.index)
            }

            // Check the privileges on the indexes - users shouldn't see each other indexes
            checkIndexAccess(AVAILABILITY_INDEX, johnClient!!, RestStatus.FORBIDDEN)
            checkIndexAccess(AVAILABILITY_INDEX, jillClient, RestStatus.FORBIDDEN)
            checkIndexAccess(AIRLINE_INDEX, janeClient, RestStatus.FORBIDDEN)

            setFilterByBackendRole(true)

            // Check if users can access only to policies belonging to exact backend group
            checkPolicies(johnClient!!, john, 1)
            checkPolicies(jillClient, jill, 1)
            checkPolicies(janeClient, jane, 1)

            // Confirm that users belonging to different roles can't see each other policies
            checkPolicyGet(AVAILABILITY_POLICY, johnClient!!, RestStatus.FORBIDDEN)

            checkPolicyGet(AVAILABILITY_POLICY, jillClient, RestStatus.FORBIDDEN)

            checkPolicyGet(AIRLINE_POLICY, janeClient, RestStatus.FORBIDDEN)
        } finally {
            jillClient?.close()
            janeClient?.close()
            deleteUser(jill)
            deleteUser(jane)
            deleteRole(PHONE_OPERATOR_ROLE)
            deleteIndexByName(AIRLINE_INDEX)
            deleteIndexByName(AVAILABILITY_INDEX)
            setFilterByBackendRole(false)
        }
    }
}
