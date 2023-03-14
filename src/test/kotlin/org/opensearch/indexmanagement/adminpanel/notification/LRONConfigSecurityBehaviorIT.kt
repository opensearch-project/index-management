/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification

import org.junit.After
import org.junit.Before
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.indexmanagement.DELETE_LRON_CONFIG
import org.opensearch.indexmanagement.GET_LRON_CONFIG
import org.opensearch.indexmanagement.GET_LRON_CONFIGS
import org.opensearch.indexmanagement.INDEX_LRON_CONFIG
import org.opensearch.indexmanagement.SecurityRestTestCase

class LRONConfigSecurityBehaviorIT : SecurityRestTestCase() {
    private val password = "password"
    private val superUser = "john"
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
        createRole(HELPDESK_ROLE, helpdeskClusterPermissions, emptyList(), emptyList())
        createUser(name = superUser, pwd = password, backendRoles = listOf(HELPDESK))
        assignRoleToUsers(HELPDESK_ROLE, listOf(superUser))
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
        deleteIndexByName(".opendistro-admin-panel-config")
    }

    fun `test index LRONConfig`() {

    }
}
