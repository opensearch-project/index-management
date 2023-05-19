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

package org.opensearch.indexmanagement.security

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
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep
import org.opensearch.indexmanagement.rollup.model.ISMRollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.RollupMetrics
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.model.metric.ValueCount
import org.opensearch.indexmanagement.security.ADD_POLICY
import org.opensearch.indexmanagement.security.BULK_WRITE_INDEX
import org.opensearch.indexmanagement.security.CREATE_INDEX
import org.opensearch.indexmanagement.security.DELETE_POLICY
import org.opensearch.indexmanagement.security.EXPLAIN_INDEX
import org.opensearch.indexmanagement.security.EXPLAIN_ROLLUP
import org.opensearch.indexmanagement.security.GET_INDEX_MAPPING
import org.opensearch.indexmanagement.security.GET_POLICIES
import org.opensearch.indexmanagement.security.GET_POLICY
import org.opensearch.indexmanagement.security.GET_ROLLUP
import org.opensearch.indexmanagement.security.INDEX_ROLLUP
import org.opensearch.indexmanagement.security.MANAGED_INDEX
import org.opensearch.indexmanagement.security.PUT_INDEX_MAPPING
import org.opensearch.indexmanagement.security.SEARCH_INDEX
import org.opensearch.indexmanagement.security.UPDATE_ROLLUP
import org.opensearch.indexmanagement.security.WRITE_INDEX
import org.opensearch.indexmanagement.security.WRITE_POLICY
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale
import org.opensearch.indexmanagement.SecurityRestTestCase

@TestLogging("level:DEBUG", reason = "Debug for tests.")
class IndexStateManagementSecurityBehaviorIT : SecurityRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)
    private val password = "Test123sdfsdfds435346FDGDFGDFG2342&^%#$@#35!"

    private val superIsmUser = "john"
    private var superUserClient: RestClient? = null

    private val testUser = "testUser"
    private val testRole = "test_role"
    var testClient: RestClient? = null

    @Before
    fun setupUsersAndRoles() {
        updateClusterSetting(ManagedIndexSettings.JITTER.key, "0.0", false)

        val helpdeskClusterPermissions = listOf(
            WRITE_POLICY,
            DELETE_POLICY,
            ADD_POLICY,
            GET_POLICY,
            GET_POLICIES,
            EXPLAIN_INDEX,
            INDEX_ROLLUP,
            GET_ROLLUP,
            EXPLAIN_ROLLUP,
            UPDATE_ROLLUP,
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
        createUser(superIsmUser, password, listOf(HELPDESK))
        createRole(HELPDESK_ROLE, helpdeskClusterPermissions, indexPermissions, listOf(AIRLINE_INDEX_PATTERN))
        assignRoleToUsers(HELPDESK_ROLE, listOf(superIsmUser))

        superUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), superIsmUser, password).setSocketTimeout(
                60000
            ).setConnectionRequestTimeout(180000)
                .build()
    }

    @After
    fun cleanup() {
        superUserClient?.close()
        deleteUser(superIsmUser)
        deleteRole(HELPDESK_ROLE)
        testClient?.close()
        deleteUser(testUser)
        deleteRole(testRole)
        deleteIndexByName(".opendistro-ism-config")
    }

    fun `test policy successful execution`() {
        // User without appropriate cluster rollup privileges
        createTestUserWithRole(emptyList(), emptyList())

        testClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .setConnectionRequestTimeout(180000)
                .build()

        val indexName = "${AIRLINE_INDEX}_index_basic"
        val targetIdxRollup = "${AIRLINE_INDEX}_target_index"

        val policyID = "${AIRLINE_POLICY}_policy_basic"

        try {
            val rollup = createISMRollup(targetIdxRollup)

            val actionConfig = RollupAction(rollup, 0)
            val states = listOf(
                State("rollup", listOf(actionConfig), listOf())
            )

            val policy = createPolicyWithRollupStep(policyID, states, indexName)

            // Assert that client without appropriate cluster privilege can't create policy
            createPolicyAndCheckStatus(policy, RestStatus.FORBIDDEN, testClient!!)
            // Create policy as john
            createPolicy(policy = policy, policyId = policyID, client = superUserClient!!)

            val sourceIndexMappingString =
                "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " + "\"keyword\" }, \"PULocationID\": { \"type\": \"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " + "{ \"type\": \"double\" }}"
            createIndex(indexName, policyID, mapping = sourceIndexMappingString)

            val managedIndexConfig = getExistingManagedIndexConfig(indexName)

            // Change the start time so that the policy will be initialized.
            updateManagedIndexConfigStartTime(managedIndexConfig)
            waitFor {
                val managedIndex = getExplainManagedIndexMetaData(indexName, superUserClient!!)
                assertEquals(policyID, managedIndex.policyID)
                assertEquals(indexName, managedIndex.index)
                // user without appropriate privileges can't access the neither the index nor the policy
                checkIndexAccess(indexName, testClient!!, RestStatus.FORBIDDEN)
                checkPolicyGet(policy.id, testClient!!, RestStatus.FORBIDDEN)
            }
            // Check if the index has rolled up
            assertIndexRolledUp(indexName, policyID, rollup)
        } finally {
            deleteIndexByName(indexName)
            deleteIndexByName(targetIdxRollup)
        }
    }

    fun `test add policy`() {
        // In this test suite case john is a "super-user" which has all relevant privileges
        createTestUserWithRole(listOf(EXPLAIN_INDEX, GET_POLICY), listOf(GET_INDEX_MAPPING, SEARCH_INDEX))

        testClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .setConnectionRequestTimeout(180000)
                .build()
        try {
            val testPolicyJson = createReplicaCountTestPolicyRequest(10, "")
            createPolicyJson(testPolicyJson, AIRLINE_POLICY, true, superUserClient!!)
            client().makeRequest("PUT", "/$AIRLINE_INDEX")

            checkPolicyGet(AIRLINE_POLICY, testClient!!, RestStatus.OK)
            checkPolicyGet(AIRLINE_POLICY, superUserClient!!, RestStatus.OK)

            addPolicyToIndex(AIRLINE_INDEX, AIRLINE_POLICY, RestStatus.FORBIDDEN, testClient!!)
            addPolicyToIndex(AIRLINE_INDEX, AIRLINE_POLICY, RestStatus.OK, superUserClient!!)
        } finally {
            deleteIndexByName(AIRLINE_INDEX)
        }
    }

    fun `test explain index`() {
        createTestUserWithRole(listOf(EXPLAIN_INDEX), listOf(GET_INDEX_MAPPING, SEARCH_INDEX))

        testClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .setConnectionRequestTimeout(180000)
                .build()
        try {
            val testPolicyJson = createReplicaCountTestPolicyRequest(10, AIRLINE_INDEX_PATTERN)
            createPolicyJson(testPolicyJson, AIRLINE_POLICY, true, client())
            client().makeRequest("PUT", "/$AIRLINE_INDEX")

            val testIndexConfig = getExistingManagedIndexConfig(AIRLINE_INDEX)
            updateManagedIndexConfigStartTime(testIndexConfig)
            waitFor {
                val airlineIndex = getExplainManagedIndexMetaData(AIRLINE_INDEX, testClient)
                assertEquals(AIRLINE_POLICY, airlineIndex.policyID)
                assertEquals(AIRLINE_INDEX, airlineIndex.index)
            }
        } finally {
            deleteIndexByName(AIRLINE_INDEX)
        }
    }

    fun `test delete policy`() {
        createTestUserWithRole(
            listOf(EXPLAIN_INDEX, GET_POLICY, EXPLAIN_INDEX),
            listOf(GET_INDEX_MAPPING, SEARCH_INDEX)
        )

        testClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .setConnectionRequestTimeout(180000)
                .build()

        try {
            val testPolicyJson = createReplicaCountTestPolicyRequest(10, AIRLINE_INDEX_PATTERN)
            createPolicyJson(testPolicyJson, AIRLINE_POLICY, true, superUserClient!!)
            checkPolicyGet(AIRLINE_POLICY, testClient!!, RestStatus.OK)
            checkPolicyGet(AIRLINE_POLICY, superUserClient!!, RestStatus.OK)

            deletePolicy(AIRLINE_POLICY, testClient!!, RestStatus.FORBIDDEN)
            deletePolicy(AIRLINE_POLICY, superUserClient!!, RestStatus.OK)

            checkPolicyGet(AIRLINE_POLICY, superUserClient!!, RestStatus.NOT_FOUND)
        } finally {
            deleteIndexByName(AIRLINE_INDEX)
        }
    }

    private fun createTestUserWithRole(clusterPermissions: List<String>, indexPermissions: List<String>) {
        val testBackendRole = testRole + "_backend"
        // In this test suite case john is a "super-user" which has all relevant privileges
        createUser(testUser, password, listOf(testBackendRole))
        createRole(testRole, clusterPermissions, indexPermissions, listOf(AIRLINE_INDEX_PATTERN))
        assignRoleToUsers(testRole, listOf(testUser))
    }

    private fun createPolicyWithRollupStep(
        policyID: String,
        states: List<State>,
        indexName: String,
    ): Policy {
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
        return policy
    }

    private fun createISMRollup(targetIdxRollup: String): ISMRollup {
        return ISMRollup(
            description = "basic search test",
            targetIndex = targetIdxRollup,
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
    }

    private fun assertIndexRolledUp(indexName: String, policyId: String, rollup: ISMRollup) {
        val rollupId = rollup.toRollup(indexName).id
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so that the policy will be initialized.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyId, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so that the rollup action will be attempted.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        Thread.sleep(60000)

        // Change the start time so that the rollup action will be attempted.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobCompletionMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
        val rollupJob = getRollup(rollupId = rollupId)
        waitFor {
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }
    }
}
