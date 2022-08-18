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
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.indexstatemanagement.action.RollupAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.rollup.model.ISMRollup
import org.opensearch.indexmanagement.rollup.model.RollupMetrics
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.model.metric.ValueCount
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

@TestLogging("level:DEBUG", reason = "Debug for tests.")
class SecurityBehaviorIT : SecurityRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    private val john = "john"
    private var johnClient: RestClient? = null

    private val jill = "jill"
    private var jillClient: RestClient? = null

    private val jane = "jane"
    private var janeClient: RestClient? = null

    private val password = "Test123!"

    override fun preserveIndicesUponCompletion(): Boolean {
        return true
    }

    @Before
    fun setupUsersAndRoles() {
        val helpdeskClusterPermissions = listOf(
            WRITE_POLICY,
            GET_POLICY,
            GET_POLICIES,
            EXPLAIN_INDEX,
            ROLLUP_ALL
        )

        val indexPermissions = listOf(
            MANAGED_INDEX,
            GET_INDEX,
            SEARCH_INDEX
        )

        val phoneOperatorClusterPermissions = listOf(
            EXPLAIN_INDEX,
            GET_POLICY,
            WRITE_POLICY,
            GET_POLICIES
        )

        createUser(john, password, listOf(HELPDESK))
        createUser(jill, password, listOf(HELPDESK))
        createRole(HELPDESK_ROLE, helpdeskClusterPermissions, indexPermissions, listOf(AIRLINE_INDEX_PATTERN))
        assignRoleToUsers(HELPDESK_ROLE, listOf(john))

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

        johnClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), john, password).setSocketTimeout(60000)
                .build()
        jillClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), jill, password).setSocketTimeout(60000)
                .build()
        janeClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), jane, password).setSocketTimeout(60000)
                .build()
    }

    @After
    fun cleanup() {
        johnClient?.close()
        jillClient?.close()
        janeClient?.close()

        deleteUser(john)
        deleteUser(jill)
        deleteUser(jane)

        deleteRole(HELPDESK_ROLE)
        deleteRole(PHONE_OPERATOR_ROLE)

        deleteIndex(".opendistro-ism-config")
    }

    fun `test security integral`() {
        setFilterByBackendRole(false)

        try {
            val airlinePolicyJson = createReplicaCountTestPolicyRequest(10, AIRLINE_INDEX_PATTERN)
            createPolicyJson(airlinePolicyJson, AIRLINE_POLICY, true, johnClient!!)

            val availabilityPolicyJson = createReplicaCountTestPolicyRequest(15, AVAILABILITY_INDEX_PATTERN)
            createPolicyJson(availabilityPolicyJson, AVAILABILITY_POLICY, true, janeClient!!)

            checkPolicies(johnClient!!, john, 2)
            // Confirm that jill can't see the policy because of missing privilege
            checkPolicies(jillClient!!, jill, null, RestStatus.FORBIDDEN)
            checkPolicies(janeClient!!, jane, 2)

            // Assign jill to airline role
            assignRoleToUsers(HELPDESK_ROLE, listOf(john, jill))
            checkPolicies(jillClient!!, jill, 2)
            // Confirm that jill can see the policy
            checkPolicyAccess(AIRLINE_POLICY, jillClient!!, RestStatus.OK)

            // Create two index - one for helpdesk service and one for phone operators
            client().makeRequest("PUT", "/$AIRLINE_INDEX")
            client().makeRequest("PUT", "/$AVAILABILITY_INDEX")

            waitFor {
                var airlineIndex = getExplainManagedIndexMetaData(AIRLINE_INDEX, johnClient!!)
                assertEquals(AIRLINE_POLICY, airlineIndex.policyID)
                assertEquals(AIRLINE_INDEX, airlineIndex.index)

                airlineIndex = getExplainManagedIndexMetaData(AIRLINE_INDEX, janeClient!!)
                assertEquals(AIRLINE_POLICY, airlineIndex.policyID)
                assertEquals(AIRLINE_INDEX, airlineIndex.index)

                val availabilityIndex = getExplainManagedIndexMetaData(AVAILABILITY_INDEX, janeClient!!)
                assertEquals(AVAILABILITY_POLICY, availabilityIndex.policyID)
                assertEquals(AVAILABILITY_INDEX, availabilityIndex.index)

                // Check the privileges on the indexes - users shouldn't see each other indexes
                checkIndexAccess(AVAILABILITY_INDEX, johnClient!!, RestStatus.FORBIDDEN)
                checkIndexAccess(AVAILABILITY_INDEX, jillClient!!, RestStatus.FORBIDDEN)
                checkIndexAccess(AIRLINE_INDEX, janeClient!!, RestStatus.FORBIDDEN)
            }
            setFilterByBackendRole(true)

            // Check if users can access only to policies belonging to exact backend group
            checkPolicies(johnClient!!, john, 1)
            checkPolicies(jillClient!!, jill, 1)
            checkPolicies(janeClient!!, jane, 1)

            // Confirm that users belonging to different roles can't see each other policies
            checkPolicyAccess(AVAILABILITY_POLICY, johnClient!!, RestStatus.FORBIDDEN)
            checkPolicyAccess(AVAILABILITY_POLICY, jillClient!!, RestStatus.FORBIDDEN)
            checkPolicyAccess(AIRLINE_POLICY, janeClient!!, RestStatus.FORBIDDEN)
        } finally {
            deleteIndex(AIRLINE_INDEX)
            deleteIndex(AVAILABILITY_INDEX)
            setFilterByBackendRole(false)
        }
    }

    fun `test create policy with rollup step against user with privileges`() {
        val user = "testUser"
        val testUserRole = "test_role"
        createUserWithCustomRole(user, password, testUserRole, emptyList(), emptyList(), emptyList(), emptyList())
        val testClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), user, password).setSocketTimeout(60000)
                .build()
        val indexName = "${AIRLINE_INDEX}_index_basic"
        val policyID = "${AIRLINE_POLICY}_policy_basic"

        try {
            val rollup = ISMRollup(
                description = "basic search test",
                targetIndex = "target_rollup_index",
                pageSize = 100,
                dimensions = listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID")
                ),
                metrics = listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics = listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average()
                        )
                    ),
                    RollupMetrics(
                        sourceField = "total_amount",
                        targetField = "total_amount",
                        metrics = listOf(Max(), Min())
                    )
                )
            )
            val actionConfig = RollupAction(rollup, 0)
            val states = listOf(
                State("rollup", listOf(actionConfig), listOf())
            )
            val sourceIndexMappingString =
                "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " + "\"keyword\" }, \"PULocationID\": { \"type\": \"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " + "{ \"type\": \"double\" }}"

            val policy = Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
                ismTemplate = listOf(
                    ISMTemplate(listOf("$indexName*"), 100, Instant.now().truncatedTo(ChronoUnit.MILLIS))
                )
            )

            createPolicy(policy = policy, policyId = policyID, client = johnClient!!)
            createIndex(indexName, sourceIndexMappingString, client())

            waitFor {
                val managedIndex = getExplainManagedIndexMetaData(indexName, johnClient!!)
                assertEquals(policyID, managedIndex.policyID)
                assertEquals(indexName, managedIndex.index)

                checkIndexAccess(indexName, testClient, RestStatus.FORBIDDEN)
                checkPolicyAccess(indexName, testClient, RestStatus.FORBIDDEN)
            }
        } finally {
            testClient.close()
            deleteUser(user)
            deleteRole(testUserRole)
            deleteIndex(indexName)
        }
    }

    fun `test create rollup against user with privileges and user without privileges`() {
        val rollupUser = "rollupUser"
        val rollupRole = "rollup_role"

        val rollupClusterPrivileges = listOf(
            INDEX_ROLLUP,
            GET_ROLLUP,
            EXPLAIN_ROLLUP
        )

        val indexPermissions = listOf(
            MANAGED_INDEX,
            GET_INDEX,
            SEARCH_INDEX
        )

        // Create user with rollup roles
        createUserWithCustomRole(
            rollupUser,
            password,
            rollupRole,
            rollupClusterPrivileges,
            indexPermissions,
            emptyList(),
            listOf("testIdxTemplate*")
        )

        val rollupUserClient =
            SecureRestClientBuilder(
                clusterHosts.toTypedArray(),
                isHttps(),
                rollupUser,
                password
            ).setSocketTimeout(60000)
                .build()

        // Create client without rollup roles assigned
        val user = "testUser"
        val testUserRole = "test_role"

        createUserWithCustomRole(
            user,
            password,
            testUserRole,
            emptyList(),
            emptyList()
        )

        val testClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), user, password).setSocketTimeout(60000)
                .build()
        try {
            val rollup = randomRollup()

            createRollup(rollup, rollupUserClient)
            waitFor {
                // Rollup user can access rollup job
                checkRollupAccess(rollup.id, rollupUserClient, RestStatus.OK)
                // Non rollup user can't access rollup job
                checkRollupAccess(rollup.id, testClient, RestStatus.FORBIDDEN)
            }

            // Assign rollup privilege to non rollup user and check if he can access the rollup job
            assignRoleToUsers(rollupRole, listOf(user, rollupUser))
            checkRollupAccess(rollup.id, testClient, RestStatus.OK)
        } finally {
            rollupUserClient.close()
            testClient.close()
            deleteUser(rollupUser)
            deleteUser(user)
            deleteRole(rollupRole)
            deleteRole(testUserRole)
        }
    }

    private fun createReplicaCountTestPolicyRequest(priority: Int, indexPattern: String): String {
        return """
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
    }

    companion object {
        private const val AIRLINE_POLICY = "airline-policy"
        private const val AVAILABILITY_POLICY = "availability-policy"

        private const val AIRLINE_INDEX = "airline-1"
        private const val AVAILABILITY_INDEX = "availability-1"

        private const val PHONE_OPERATOR = "phone_operator"
        private const val HELPDESK = "helpdesk_stuff"
        private const val HELPDESK_ROLE = "helpdesk_role"
        private const val PHONE_OPERATOR_ROLE = "phone_operator_role"

        private const val AIRLINE_INDEX_PATTERN = "airline-*"
        private const val AVAILABILITY_INDEX_PATTERN = "availability-*"
    }
}
