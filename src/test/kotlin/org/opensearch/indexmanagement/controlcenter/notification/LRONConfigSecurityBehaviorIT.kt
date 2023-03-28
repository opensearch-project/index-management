/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification

import org.junit.After
import org.junit.Before
import org.opensearch.client.Request
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.indexmanagement.DELETE_LRON_CONFIG
import org.opensearch.indexmanagement.GET_LRON_CONFIG
import org.opensearch.indexmanagement.INDEX_LRON_CONFIG
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.SecurityRestTestCase
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
            DELETE_LRON_CONFIG
        )

        // In this test suite case john is a "super-user" which has all relevant privileges
        createUser(name = superUser, pwd = password)
        createAndAssignRole(HELPDESK_ROLE, helpdeskClusterPermissions, superUser)
        superUserClient =
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), superUser, password).setSocketTimeout(
                60000
            ).setConnectionRequestTimeout(180000)
                .build()

        createUser(name = testUser, pwd = password)
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
        deleteIndexByName(IndexManagementPlugin.CONTROL_CENTER_INDEX)
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
        } finally {
            deleteRole(indexConfigRole)
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
        } finally {
            deleteRole(indexConfigRole)
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
        } finally {
            deleteRole(deleteConfigRole)
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
        } finally {
            deleteRole(getConfigRole)
        }
    }

    fun `test get LRONConfigs with security`() {
        /* super user */
        val createRequest = Request("POST", IndexManagementPlugin.LRON_BASE_URI)
        randomList(1, 15) {
            createRequest.setJsonEntity(randomLRONConfig().toJsonString())
            executeRequest(createRequest, RestStatus.OK, superUserClient!!).asMap()
        }

        val getRequest = Request("GET", IndexManagementPlugin.LRON_BASE_URI)
        executeRequest(getRequest, RestStatus.OK, superUserClient!!)

        /* test user */
        executeRequest(getRequest, RestStatus.FORBIDDEN, testUserClient!!)

        val getConfigRole = "get_lron_config"
        try {
            createAndAssignRole(getConfigRole, listOf(GET_LRON_CONFIG), testUser)
            executeRequest(getRequest, RestStatus.OK, testUserClient!!)
        } finally {
            deleteRole(getConfigRole)
        }
    }

    private fun createAndAssignRole(roleName: String, permissions: List<String>, user: String) {
        createRole(roleName, permissions, emptyList(), emptyList())
        assignRoleToUsers(roleName, listOf(user))
    }
}
