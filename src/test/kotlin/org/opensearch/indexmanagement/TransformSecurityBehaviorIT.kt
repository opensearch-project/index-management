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
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.time.temporal.ChronoUnit

@TestLogging("level:DEBUG", reason = "Debug for tests.")
class TransformSecurityBehaviorIT : SecurityRestTestCase() {
    private val password = "Test123!"

    private val superTransformUser = "john"
    private var superUserClient: RestClient? = null

    private val testUser = "testUser"
    private val testRole = "test_role"
    var testUserClient: RestClient? = null

    @Before
    fun setupUsersAndRoles() {
        updateClusterSetting(ManagedIndexSettings.JITTER.key, "0.0", false)

        // Init super transform user
        val helpdeskClusterPermissions = listOf(
            STOP_TRANSFORM,
            EXPLAIN_INDEX,
            TRANSFORM_ACTION,
            GET_TRANSFORM,
            EXPLAIN_TRANSFORM,
            START_TRANSFORM,
            DELETE_TRANSFORM,
            HEALTH,
            GET_TRANSFORMS
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
        createUser(superTransformUser, password, listOf(HELPDESK))
        createRole(HELPDESK_ROLE, helpdeskClusterPermissions, indexPermissions, listOf(AIRLINE_INDEX_PATTERN))
        assignRoleToUsers(HELPDESK_ROLE, listOf(superTransformUser))

        superUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), superTransformUser, password).setSocketTimeout(60000)
                .build()
    }

    @After
    fun cleanup() {
        // Remove super user
        superUserClient?.close()
        deleteUser(superTransformUser)
        deleteRole(HELPDESK_ROLE)
        // Remove test user
        testUserClient?.close()
        deleteUser(testUser)
        deleteRole(testRole)

        deleteIndexByName(".opendistro-ism-config")
    }

    fun `test transform successful execution`() {
        // Prepare index data
        val sourceIndex = AIRLINE_INDEX
        val targetIndex = "${AIRLINE_INDEX}_target_index"
        validateSourceIndex(sourceIndex)
        createTestUserWithRole(emptyList(), emptyList())

        testUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
            .build()

        try {
            val transform = createSimpleTransform(sourceIndex, targetIndex)
            // Create transform with user that has cluster privilege
            createTransform(transform, transform.id, true, superUserClient)
            updateTransformStartTime(transform)

            waitFor {
                assertTrue("Target transform index was not created", indexExists(transform.targetIndex))
                checkTransformExplain(transform.id, superUserClient!!, RestStatus.OK)
                checkTransformExplain(transform.id, testUserClient!!, RestStatus.FORBIDDEN)
            }
            // Check if transform was executed correctly
            waitFor {
                val job = getTransform(transformId = transform.id, client = superUserClient!!)
                assertNotNull("Transform job doesn't have metadata set", job.metadataId)
                val transformMetadata = getTransformMetadata(job.metadataId!!)
                assertEquals("Transform has not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
            }
        } finally {
            deleteIndexByName(sourceIndex)
            deleteIndexByName(targetIndex)
        }
    }

    fun `test failed transform creation user missing cluster privileges`() {
        // Prepare index data
        val sourceIndex = AIRLINE_INDEX
        validateSourceIndex(sourceIndex)

        createTestUserWithRole(emptyList(), emptyList())

        testUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .build()

        try {
            val transform = randomTransform()
            // Assert that client without appropriate cluster privilege can't create transform
            createTransformAndCheckStatus(transform, RestStatus.FORBIDDEN, testUserClient!!)
        } finally {
            deleteIndexByName(sourceIndex)
        }
    }

    fun `test failed transform execution user missing index access`() {
        // Prepare index data
        val sourceIndex = "source_index"
        val targetIndex = "target_index"
        validateSourceIndex(sourceIndex)

        try {
            val transform = createSimpleTransform(sourceIndex, targetIndex)
            createTransform(transform, transform.id, true, superUserClient)
            // Verify that user can access the transform
            checkTransformExplain(transform.id, superUserClient!!, RestStatus.OK)
            // Create transform with user that has cluster privilege
            updateTransformStartTime(transform)

            waitFor {
                val job = getTransform(transformId = transform.id, client = superUserClient!!)
                assertNotNull("Transform job doesn't have metadata set", job.metadataId)
                val transformMetadata = getTransformMetadata(job.metadataId!!)
                assertEquals("Transform finished", TransformMetadata.Status.FAILED, transformMetadata.status)
                assertFalse("Target transform index was created", indexExists(transform.targetIndex))
            }
        } finally {
            deleteIndexByName(sourceIndex)
        }
    }

    fun `test transform access`() {
        // Prepare index data
        val sourceIndex = AIRLINE_INDEX
        val targetIndex = "${AIRLINE_INDEX}_target_index"
        validateSourceIndex(sourceIndex)

        // Create client without rollup roles assigned
        createTestUserWithRole(emptyList(), emptyList())
        testUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .build()
        try {
            val transform = createSimpleTransform(sourceIndex, targetIndex)
            // User without cluster privileges can't create transform
            createTransformAndCheckStatus(transform, RestStatus.FORBIDDEN, testUserClient!!)

            createTransformAndCheckStatus(transform, RestStatus.CREATED, superUserClient!!)

            // User with transform explain can access transform job
            checkTransformExplain(transform.id, superUserClient!!, RestStatus.OK)
            checkTransformGet(transform.id, superUserClient!!, RestStatus.OK)
            // User without transform explain will receive FORBIDDEN
            checkTransformExplain(transform.id, testUserClient!!, RestStatus.FORBIDDEN)
            checkTransformGet(transform.id, testUserClient!!, RestStatus.FORBIDDEN)

            // Assign transform related privileges to non transform user and check if he can access the transform
            assignRoleToUsers(HELPDESK_ROLE, listOf(testUser, superTransformUser))

            checkTransformExplain(transform.id, testUserClient!!, RestStatus.OK)
            checkTransformGet(transform.id, testUserClient!!, RestStatus.OK)
        } finally {
            deleteIndexByName(sourceIndex)
            deleteIndexByName(targetIndex)
        }
    }

    fun `test delete transform`() {
        createTestUserWithRole(listOf(GET_TRANSFORM), listOf(GET_INDEX_MAPPING, SEARCH_INDEX))

        testUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .build()

        val targetIndex = "$AIRLINE_INDEX-target"
        try {
            val transform = createSimpleTransform(AIRLINE_INDEX, targetIndex)
            createTransform(transform, transform.id, true, superUserClient!!)

            checkTransformGet(transform.id, superUserClient!!, RestStatus.OK)
            checkTransformGet(transform.id, testUserClient!!, RestStatus.OK)

            deleteTransform(transform.id, testUserClient!!, RestStatus.FORBIDDEN)
            // Stop transform in order to delete it
            stopTransform(transform.id, superUserClient!!, RestStatus.OK)
            deleteTransform(transform.id, superUserClient!!, RestStatus.OK)

            checkTransformGet(transform.id, superUserClient!!, RestStatus.NOT_FOUND)
        } finally {
            deleteIndexByName(AIRLINE_INDEX)
        }
    }

    fun `test stop transform`() {
        createTestUserWithRole(listOf(GET_TRANSFORM), listOf(GET_INDEX_MAPPING, SEARCH_INDEX))

        testUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(60000)
                .build()

        val targetIndex = "$AIRLINE_INDEX-target"
        try {
            val transform = createSimpleTransform(AIRLINE_INDEX, targetIndex)
            createTransform(transform, transform.id, true, superUserClient!!)

            checkTransformGet(transform.id, superUserClient!!, RestStatus.OK)
            checkTransformGet(transform.id, testUserClient!!, RestStatus.OK)

            stopTransform(transform.id, testUserClient!!, RestStatus.FORBIDDEN)
            stopTransform(transform.id, superUserClient!!, RestStatus.OK)

            assignRoleToUsers(HELPDESK_ROLE, listOf(testUser, superTransformUser))
            stopTransform(transform.id, testUserClient!!, RestStatus.OK)
        } finally {
            deleteIndexByName(AIRLINE_INDEX)
        }
    }

    private fun createSimpleTransform(
        sourceIndex: String,
        targetIndex: String,
    ): Transform {
        return Transform(
            id = "id_1",
            schemaVersion = 1L,
            enabled = true,
            enabledAt = Instant.now(),
            updatedAt = Instant.now(),
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            description = "test transform",
            metadataId = null,
            sourceIndex = sourceIndex,
            targetIndex = targetIndex,
            roles = emptyList(),
            pageSize = 100,
            groups = listOf(
                Terms(sourceField = "store_and_fwd_flag", targetField = "flag")
            )
        )
    }

    private fun createTestUserWithRole(clusterPermissions: List<String>, indexPermissions: List<String>) {
        val testBackendRole = testRole + "_backend"
        // In this test suite case john is a "super-user" which has all relevant privileges
        createUser(testUser, password, listOf(testBackendRole))
        createRole(testRole, clusterPermissions, indexPermissions, listOf(AIRLINE_INDEX_PATTERN))
        assignRoleToUsers(testRole, listOf(testUser))
    }
}
