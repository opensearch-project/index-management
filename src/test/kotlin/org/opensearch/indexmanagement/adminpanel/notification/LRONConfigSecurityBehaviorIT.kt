/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification

import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.opensearch.client.Request
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.indexmanagement.DELETE_LRON_CONFIG
import org.opensearch.indexmanagement.GET_LRON_CONFIG
import org.opensearch.indexmanagement.GET_LRON_CONFIGS
import org.opensearch.indexmanagement.INDEX_LRON_CONFIG
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.SecurityRestTestCase
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.rest.RestStatus

@Suppress("UNCHECKED_CAST")
class LRONConfigSecurityBehaviorIT : SecurityRestTestCase() {
    private val password = "password"
    private val superUser = "superUser"
    private var superUserClient: RestClient? = null

    private val testUser = "testUser"
    private val testRole = "test"
    var testUserClient: RestClient? = null

    @Before
    fun setupUsersAndRoles() {
        // Init super user
        val helpdeskClusterPermissions = listOf(
            INDEX_LRON_CONFIG,
            GET_LRON_CONFIG,
            GET_LRON_CONFIGS,
            DELETE_LRON_CONFIG
        )

        // In this test suite case john is a "super-user" which has all relevant privileges
        createUser(name = superUser, pwd = password, backendRoles = listOf(HELPDESK))
        createAndAssignRole(HELPDESK_ROLE, helpdeskClusterPermissions, superUser)
        superUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), superUser, password).setSocketTimeout(
                60000
            ).setConnectionRequestTimeout(180000)
                .build()

        createUser(name = testUser, pwd = password, backendRoles = emptyList())
        testUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), testUser, password).setSocketTimeout(
                60000
            ).setConnectionRequestTimeout(180000)
                .build()
    }

    @After
    fun cleanup() {
        // Remove super user
        superUserClient?.close()
        deleteUser(superUser)
        deleteRole(HELPDESK_ROLE)
        // Remove test user
        testUserClient?.close()
        deleteUser(testUser)
        deleteRole(testRole)
        deleteIndexByName(IndexManagementPlugin.ADMIN_PANEL_INDEX)
    }

    fun `test index LRONConfig with security`() {
        /* super user */
        val request = Request("POST", IndexManagementPlugin.LRON_BASE_URI)
        request.setJsonEntity(randomLRONConfig().toJsonString())
        executeRequest(request, RestStatus.OK, superUserClient!!)
        /* test user */
        request.setJsonEntity(randomLRONConfig().toJsonString())
        executeRequest(request, RestStatus.FORBIDDEN, testUserClient!!)

        val indexConfigRole = "index_lron_config"
        try {
            createAndAssignRole(indexConfigRole, listOf(INDEX_LRON_CONFIG), testUser)
            executeRequest(request, RestStatus.OK, testUserClient!!)
            request.setJsonEntity(randomLRONConfig().toJsonString())

            setFilterByBackendRole(true)
            executeRequest(request, RestStatus.FORBIDDEN, testUserClient!!)
            /* we can also use create method to call put api to update user info*/
            createUserWithCustomRole(
                user = testUser,
                password = password,
                role = indexConfigRole,
                backendRoles = listOf(HELPDESK)
            )
            executeRequest(request, RestStatus.OK, testUserClient!!)
        } finally {
            deleteRole(indexConfigRole)
            setFilterByBackendRole(false)
        }
    }

    fun `test update LRONConfig with security`() {
        /* super user */
        val lronConfig = randomLRONConfig()
        val createRequest = Request("POST", IndexManagementPlugin.LRON_BASE_URI)
        createRequest.setJsonEntity(lronConfig.toJsonString())
        executeRequest(createRequest, RestStatus.OK, superUserClient!!)
        val updateRequest = Request("PUT", getResourceURI(lronConfig.taskId, lronConfig.actionName))
        updateRequest.setJsonEntity(randomLRONConfig(taskId = lronConfig.taskId, actionName = lronConfig.actionName).toJsonString())
        executeRequest(updateRequest, RestStatus.OK, superUserClient!!)

        /* test user */
        executeRequest(updateRequest, RestStatus.FORBIDDEN, testUserClient!!)

        val indexConfigRole = "index_lron_config"
        try {
            createAndAssignRole(indexConfigRole, listOf(INDEX_LRON_CONFIG), testUser)
            executeRequest(updateRequest, RestStatus.OK, testUserClient!!)

            /* current doc was updated by testUser who doesn't have backend role */
            /* we reset it with superUser before test filterByEnabled */
            executeRequest(updateRequest, RestStatus.OK, superUserClient!!)
            setFilterByBackendRole(true)
            executeRequest(updateRequest, RestStatus.FORBIDDEN, testUserClient!!)
            /* we can also use create method to call put api to update user info*/
            createUserWithCustomRole(
                user = testUser,
                password = password,
                role = indexConfigRole,
                backendRoles = listOf(HELPDESK)
            )
            executeRequest(updateRequest, RestStatus.OK, testUserClient!!)
        } finally {
            deleteRole(indexConfigRole)
            setFilterByBackendRole(false)
        }
    }

    fun `test delete LRONConfig with security`() {
        /* super user */
        val lronConfig = randomLRONConfig()
        val createRequest = Request("POST", IndexManagementPlugin.LRON_BASE_URI)
        createRequest.setJsonEntity(lronConfig.toJsonString())
        executeRequest(createRequest, RestStatus.OK, superUserClient!!)
        val deleteRequest = Request("DELETE", getResourceURI(lronConfig.taskId, lronConfig.actionName))
        executeRequest(deleteRequest, RestStatus.OK, superUserClient!!)

        /* test user */
        executeRequest(createRequest, RestStatus.OK, superUserClient!!)
        executeRequest(deleteRequest, RestStatus.FORBIDDEN, testUserClient!!)

        val deleteConfigRole = "delete_lron_config"
        try {
            createAndAssignRole(deleteConfigRole, listOf(DELETE_LRON_CONFIG), testUser)
            executeRequest(deleteRequest, RestStatus.OK, testUserClient!!)

            setFilterByBackendRole(true)
            executeRequest(createRequest, RestStatus.OK, superUserClient!!)
            executeRequest(deleteRequest, RestStatus.FORBIDDEN, testUserClient!!)
            /* we can also use create method to call put api to update user info*/
            createUserWithCustomRole(
                user = testUser,
                password = password,
                role = deleteConfigRole,
                backendRoles = listOf(HELPDESK)
            )
            executeRequest(deleteRequest, RestStatus.OK, testUserClient!!)
        } finally {
            deleteRole(deleteConfigRole)
            setFilterByBackendRole(false)
        }
    }

    fun `test get LRONConfig with security`() {
        /* super user */
        val lronConfig = randomLRONConfig()
        val createRequest = Request("POST", IndexManagementPlugin.LRON_BASE_URI)
        createRequest.setJsonEntity(lronConfig.toJsonString())
        executeRequest(createRequest, RestStatus.OK, superUserClient!!)
        val getRequest = Request("GET", getResourceURI(lronConfig.taskId, lronConfig.actionName))
        executeRequest(getRequest, RestStatus.OK, superUserClient!!)

        /* test user */
        executeRequest(getRequest, RestStatus.FORBIDDEN, testUserClient!!)

        val getConfigRole = "get_lron_config"
        try {
            createAndAssignRole(getConfigRole, listOf(GET_LRON_CONFIG), testUser)
            executeRequest(getRequest, RestStatus.OK, testUserClient!!)

            setFilterByBackendRole(true)
            executeRequest(getRequest, RestStatus.FORBIDDEN, testUserClient!!)
            /* we can also use create method to call put api to update user info*/
            createUserWithCustomRole(
                user = testUser,
                password = password,
                role = getConfigRole,
                backendRoles = listOf(HELPDESK)
            )
            executeRequest(getRequest, RestStatus.OK, testUserClient!!)
        } finally {
            deleteRole(getConfigRole)
            setFilterByBackendRole(false)
        }
    }

    fun `test get LRONConfigs with security`() {
        /* super user */
        val createRequest = Request("POST", IndexManagementPlugin.LRON_BASE_URI)
        val lronConfigResponses = randomList(1, 15) {
            createRequest.setJsonEntity(randomLRONConfig().toJsonString())
            executeRequest(createRequest, RestStatus.OK, superUserClient!!).asMap()
        }

        val getRequest = Request("GET", IndexManagementPlugin.LRON_BASE_URI)
        executeRequest(getRequest, RestStatus.OK, superUserClient!!)

        /* test user */
        executeRequest(getRequest, RestStatus.FORBIDDEN, testUserClient!!)

        val getConfigsRole = "get_lron_configs"
        val anotherUser = "another"
        val anotherBackendRole = "backendRoleForTest"
        var anotherUserClient: RestClient? = null
        try {
            createAndAssignRole(getConfigsRole, listOf(GET_LRON_CONFIGS), testUser)
            executeRequest(getRequest, RestStatus.OK, testUserClient!!)

            setFilterByBackendRole(true)
            executeRequest(getRequest, RestStatus.FORBIDDEN, testUserClient!!)

            /* we add other backend role to test the filterByBackendRole feature */
            createUserWithCustomRole(
                user = anotherUser,
                password = password,
                role = HELPDESK_ROLE,
                backendRoles = listOf(anotherBackendRole)
            )
            anotherUserClient =
                SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), anotherUser, password).setSocketTimeout(
                    60000
                ).setConnectionRequestTimeout(180000)
                    .build()
            val anotherLRONConfigResponses = randomList(1, 15) {
                createRequest.setJsonEntity(randomLRONConfig().toJsonString())
                executeRequest(createRequest, RestStatus.OK, anotherUserClient!!).asMap()
            }
            /* we can also use create method to call put api to update user info*/
            createUserWithCustomRole(
                user = testUser,
                password = password,
                role = getConfigsRole,
                backendRoles = listOf(HELPDESK)
            )

            val testUserResponses = executeRequest(getRequest, RestStatus.OK, testUserClient!!)
                .asMap()["lron_configs"] as List<Map<String, Any?>>
            for (lronConfigResponse in lronConfigResponses) {
                val resLRONConfigResponse = testUserResponses.find { lronConfigResponse["_id"] as String == it["_id"] as String }
                assertEquals(
                    "different lronConfigResponse",
                    lronConfigResponse[LRONConfig.LRON_CONFIG_FIELD],
                    resLRONConfigResponse!![LRONConfig.LRON_CONFIG_FIELD]
                )
            }
            for (lronConfigResponse in anotherLRONConfigResponses) {
                val resLRONConfigResponse = testUserResponses.find { lronConfigResponse["_id"] as String == it["_id"] as String }
                Assert.assertNull("shouldn't get this LRONConfig", resLRONConfigResponse)
            }
        } finally {
            deleteRole(getConfigsRole)
            setFilterByBackendRole(false)
            deleteUser(anotherUser)
            anotherUserClient?.close()
        }
    }

    private fun createAndAssignRole(roleName: String, permissions: List<String>, user: String) {
        createRole(roleName, permissions, emptyList(), emptyList())
        assignRoleToUsers(roleName, listOf(user))
    }
}
