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
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.RollupMetrics
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.model.metric.ValueCount
import org.opensearch.indexmanagement.rollup.randomRollup
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit

@TestLogging("level:DEBUG", reason = "Debug for tests.")
class RollupSecurityBehaviorIT : SecurityRestTestCase() {
    private val password = "Test123!"

    private val superRollupUser = "john"
    private var superUserClient: RestClient? = null

    private val testUser = "testUser"
    private val testRole = "test_role"
    var testUserClient: RestClient? = null

    override fun preserveIndicesUponCompletion(): Boolean {
        return true
    }

    @Before
    fun setupUsersAndRoles() {
        updateClusterSetting(ManagedIndexSettings.JITTER.key, "0.0", false)

        // Init super transform user
        val helpdeskClusterPermissions = listOf(
            INDEX_ROLLUP,
            GET_ROLLUP,
            EXPLAIN_ROLLUP,
            UPDATE_ROLLUP,
            DELETE_ROLLUP

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
        createUser(superRollupUser, password, listOf(HELPDESK))
        createRole(HELPDESK_ROLE, helpdeskClusterPermissions, indexPermissions, listOf(AIRLINE_INDEX_PATTERN))
        assignRoleToUsers(HELPDESK_ROLE, listOf(superRollupUser))

        superUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), superRollupUser, password).setSocketTimeout(60000)
                .build()
    }

    @After
    fun cleanup() {
        // Remove super user
        superUserClient?.close()
        deleteUser(superRollupUser)
        deleteRole(HELPDESK_ROLE)
        // Remove test user
        testUserClient?.close()
        deleteUser(testUser)
        deleteRole(testRole)

        deleteIndexByName(".opendistro-ism-config")
    }

    fun `test rollup successful execution`() {
        // User john can access both source and target indexes
        val sourceIndex = AIRLINE_INDEX
        val targetIndex = "${AIRLINE_INDEX}_target_runner"

        try {
            generateNYCTaxiData(sourceIndex)

            val rollup = createRollup(createBasicStatsCheckTestRollup(sourceIndex, targetIndex), superUserClient!!)

            updateRollupStartTime(rollup)

            waitFor {
                val rollupJob = getRollup(rollupId = rollup.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
                assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex))
            }
        } finally {
            deleteIndexByName(sourceIndex)
            deleteIndexByName(targetIndex)
        }
    }

    fun `test rollup access`() {
        createTestUserWithRole(emptyList(), emptyList())

        testUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .build()

        val rollup = randomRollup()

        createRollupAndCheckStatus(rollup, RestStatus.FORBIDDEN, testUserClient!!)
        createRollupAndCheckStatus(rollup, RestStatus.CREATED, superUserClient!!)

        waitFor {
            // Rollup user can access rollup job
            checkRollupExplain(rollup.id, superUserClient!!, RestStatus.OK)
            checkRollupGet(rollup.id, superUserClient!!, RestStatus.OK)

            // Non rollup user can't access rollup job
            checkRollupExplain(rollup.id, testUserClient!!, RestStatus.FORBIDDEN)
            checkRollupGet(rollup.id, testUserClient!!, RestStatus.FORBIDDEN)
        }

        // Assign rollup privilege to non rollup user and check if he can access the rollup job
        assignRoleToUsers(HELPDESK_ROLE, listOf(testUser, superRollupUser))

        checkRollupExplain(rollup.id, testUserClient!!, RestStatus.OK)
        checkRollupGet(rollup.id, testUserClient!!, RestStatus.OK)
    }

    fun `test failed rollup creation user missing cluster privileges`() {
        createTestUserWithRole(emptyList(), emptyList())

        testUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .build()

        val rollup = randomRollup()
        createRollupAndCheckStatus(rollup, RestStatus.FORBIDDEN, testUserClient!!)
    }

    fun `test failed rollup execution user missing index access`() {
        val indexName = "test_index_runner"
        val targetIndexName = "${indexName}_target"

        try {
            var rollup = createBasicStatsCheckTestRollup(indexName, targetIndexName)
            // Create source index based on the data from rollup
            createRollupSourceIndex(rollup)
            // Add document to be sure that metadata is generated
            putDateDocumentInSourceIndex(rollup)

            // Create rollup as john over index for which john doesn't have a privilege
            rollup = createRollup(rollup, superUserClient!!)
            assertEquals(indexName, rollup.sourceIndex)
            assertEquals(null, rollup.metadataID)

            // Update rollup start time to run first execution
            updateRollupStartTime(rollup)

            waitFor {
                val rollupJob = getRollup(rollupId = rollup.id)
                assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
                val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
                // Rollup can't be started over index for which user doesn't have a privilege
                assertEquals("Rollup is finished", RollupMetadata.Status.FAILED, rollupMetadata.status)
                // Confirm that john's rollup is disabled
                assertTrue("Rollup is disabled", !rollupJob.enabled)
                assertFalse("Target rollup index was created", indexExists(rollup.targetIndex))
            }
        } finally {
            deleteIndexByName(indexName)
        }
    }

    fun `test delete rollup`() {
        createTestUserWithRole(listOf(GET_ROLLUP), listOf(GET_INDEX_MAPPING, SEARCH_INDEX))

        testUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .build()
        val rollup = randomRollup()

        createRollupAndCheckStatus(rollup, RestStatus.FORBIDDEN, testUserClient!!)
        createRollupAndCheckStatus(rollup, RestStatus.CREATED, superUserClient!!)

        checkRollupGet(rollup.id, superUserClient!!, RestStatus.OK)
        checkRollupGet(rollup.id, testUserClient!!, RestStatus.OK)

        deleteRollup(rollup.id, testUserClient!!, RestStatus.FORBIDDEN)
        deleteRollup(rollup.id, superUserClient!!, RestStatus.OK)

        checkRollupGet(rollup.id, superUserClient!!, RestStatus.NOT_FOUND)
    }

    private fun createBasicStatsCheckTestRollup(
        sourceIndex: String,
        targetIndex: String,
    ) = Rollup(
        id = "basic_stats_check_runner",
        schemaVersion = 1L,
        enabled = true,
        jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
        jobLastUpdatedTime = Instant.now(),
        jobEnabledTime = Instant.now(),
        description = "basic stats test",
        sourceIndex = sourceIndex,
        targetIndex = targetIndex,
        metadataID = null,
        roles = emptyList(),
        pageSize = 100,
        delay = 0,
        continuous = false,
        dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
        metrics = listOf(
            RollupMetrics(
                sourceField = "passenger_count",
                targetField = "passenger_count",
                metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())
            )
        )
    )

    private fun createTestUserWithRole(clusterPermissions: List<String>, indexPermissions: List<String>) {
        val testBackendRole = testRole + "_backend"
        // In this test suite case john is a "super-user" which has all relevant privileges
        createUser(testUser, password, listOf(testBackendRole))
        createRole(testRole, clusterPermissions, indexPermissions, listOf(AIRLINE_INDEX_PATTERN))
        assignRoleToUsers(testRole, listOf(testUser))
    }
}
