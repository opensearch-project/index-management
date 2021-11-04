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

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.junit.Assert
import org.opensearch.client.Request
import org.opensearch.client.Response
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
class SecurityBehaviorIT : IndexManagementRestTestCase() {

    var financeUserClient: RestClient? = null
    var hrUserClient: RestClient? = null
    var adminUserClient: RestClient? = null
    var noAuthUserClient: RestClient? = null

    fun `test security behavior for ISM`() {
        setupUsersAndRoles()

        disableFilterBy()
        var financeResponse = createPolicy("finance-policy", 10, financeUserClient)
        var hrResponse = createPolicy("hr-policy", 15, hrUserClient)
        var adminResponse = createPolicy("admin-policy", 0, adminUserClient)
        var noAuthResponse = createPolicy("noauth-policy", 100, noAuthUserClient)

        assertEquals("User jane failed to create policy", RestStatus.CREATED, financeResponse?.restStatus())
        assertEquals("User jack failed to create policy", RestStatus.CREATED, hrResponse?.restStatus())
        assertEquals("User sam failed to create policy", RestStatus.CREATED, adminResponse?.restStatus())
        assertEquals("User noauth didn't fail to create policy", RestStatus.FORBIDDEN, noAuthResponse?.restStatus())

        financeResponse = getPolicies(financeUserClient)
        hrResponse = getPolicies(hrUserClient)
        adminResponse = getPolicies(adminUserClient)
        noAuthResponse = getPolicies(noAuthUserClient)

        assertEquals("User jane cannot get policies", RestStatus.OK, financeResponse?.restStatus())
        assertEquals("User jack cannot get policies", RestStatus.OK, hrResponse?.restStatus())
        assertEquals("User sam cannot get policies", RestStatus.OK, adminResponse?.restStatus())
        assertEquals("User noauth can get policies", RestStatus.FORBIDDEN, noAuthResponse?.restStatus())

        // Ensure all users can see each other policies
        assertEquals("User jane not able to see all policies", 3, financeResponse?.asMap()?.get("total_policies"))
        assertEquals("User jack not able to see all policies", 3, hrResponse?.asMap()?.get("total_policies"))
        assertEquals("User sam not able to see all policies", 3, adminResponse?.asMap()?.get("total_policies"))

        client().performRequest(Request("PUT", "/finance-1"))
        client().performRequest(Request("PUT", "/hr-1"))
        client().performRequest(Request("PUT", "/marketing-1"))

        financeResponse = explainManagedIndices(financeUserClient)
        hrResponse = explainManagedIndices(hrUserClient)
        adminResponse = explainManagedIndices(adminUserClient)
        noAuthResponse = explainManagedIndices(noAuthUserClient)

        assertEquals("User jane cannot get managed indices", RestStatus.OK, financeResponse?.restStatus())
        assertEquals("User jack cannot get managed indices", RestStatus.OK, hrResponse?.restStatus())
        assertEquals("User sam cannot get managed indices", RestStatus.OK, adminResponse?.restStatus())
        assertEquals("User noauth can get managed indices", RestStatus.FORBIDDEN, noAuthResponse?.restStatus())

        assertEquals("User jane seeing more managed indices than allowed", 1, financeResponse?.asMap()?.get("total_managed_indices"))
        assertEquals("User jack seeing more managed indices than allowed", 1, hrResponse?.asMap()?.get("total_managed_indices"))
        assertEquals("User sam seeing more managed indices than allowed", 3, adminResponse?.asMap()?.get("total_managed_indices"))

        // Enabling backend role filtering
        enableFilterBy()
        financeResponse = getPolicies(financeUserClient)
        hrResponse = getPolicies(hrUserClient)
        adminResponse = getPolicies(adminUserClient)

        // Only admin can all policies other users only can see intersecting policies
        assertEquals("User jane not able to see all policies", 2, financeResponse?.asMap()?.get("total_policies"))
        assertEquals("User jack not able to see all policies", 1, hrResponse?.asMap()?.get("total_policies"))
        assertEquals("User sam not able to see all policies", 3, adminResponse?.asMap()?.get("total_policies"))

        disableFilterBy()
    }

    private fun createPolicy(name: String, priority: Int, userClient: RestClient?): Response? {
        val request = Request("PUT", "_plugins/_ism/policies/$name")
        val json = """
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
                        "index_patterns": ["*"],
                        "priority": $priority
                    }
                }
            }
        """.trimIndent()
        request.setJsonEntity(json)
        return userClient?.performRequest(request)
    }

    private fun getPolicies(userClient: RestClient?): Response? {
        val request = Request("GET", "_plugins/_ism/policies")
        return userClient?.performRequest(request)
    }

    private fun explainManagedIndices(userClient: RestClient?): Response? {
        val request = Request("GET", "_plugins/_ism/explain")
        return userClient?.performRequest(request)
    }

    private fun createUser(name: String, pwd: String = "Test123!", backendRoles: List<String> = listOf()) {
        val request = Request("PUT", "_plugins/_security/api/internalusers/$name")
        val backendRolesStr = backendRoles.joinToString(",")
        val json = """
            {
                "password": $pwd,
                "backend_roles": [$backendRolesStr],
                "attributes":{}
            }
        """.trimIndent()
        request.setJsonEntity(json)
        client().performRequest(request)
    }

    private fun enableFilterBy() {
        val setting = """
            {
                "persistent": {
                    "plugins.index_management.filter_by_backend_roles": "true"
                } 
            }
        """.trimIndent()
        val updateResponse = client().makeRequest("PUT", "_cluster/settings", emptyMap(), StringEntity(setting, ContentType.APPLICATION_JSON))
        assertEquals(updateResponse.statusLine.toString(), 200, updateResponse.statusLine.statusCode)
    }

    private fun disableFilterBy() {
        val setting = """
            {
                "persistent": {
                    "plugins.index_management.filter_by_backend_roles": "false"
                }
            }
        """.trimIndent()
        val updateResponse = client().makeRequest("PUT", "_cluster/settings", emptyMap(), StringEntity(setting, ContentType.APPLICATION_JSON))
        Assert.assertEquals(updateResponse.statusLine.toString(), 200, updateResponse.statusLine.statusCode)
    }

    private fun addUsersToRole(role: String, users: List<String>) {
        val request = Request("PUT", "/_plugins/_security/api/rolesmapping/$role")
        val usersStr = users.joinToString(",")
        var entity = """
            {
                "backend_roles": [],
                "hosts": [],
                "users": [$usersStr]
            }
        """.trimIndent()
        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    private fun addRole(name: String, clusterPermissions: List<String>, indexPatterns: List<String>, indexPermissions: List<String>) {
        val request = Request("PUT", "/_plugins/_security/api/roles/$name")
        val indexPatternsStr = indexPatterns.joinToString(",")
        val clusterPermissionsStr = clusterPermissions.joinToString(",")
        val indexPermissionsStr = indexPermissions.joinToString(",")
        val entity = """
            {
                "cluster_permissions": [$clusterPermissionsStr],
                "index_permissions": [
                {
                    "fls": [],
                    "masked_fields": [],
                    "allowed_actions": [$indexPermissionsStr],
                    "index_patterns": [$indexPatternsStr]
                }
                ],
                "tenant_permissions": []
            }
        """.trimIndent()

        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    private fun setupUsersAndRoles() {
        // Create user jane with backend roles - ["finance", "general"]
        createUser("jane", backendRoles = listOf("finance", "hr"))

        // Create user jack with backend roles - ["hr"]
        createUser("jack", backendRoles = listOf("hr"))

        // Create user sam with backend roles - ["general"]
        createUser("sam", backendRoles = listOf("general"))

        // Create user auth with no backend roles
        createUser("noauth")

        val clusterPermissions = listOf(
            "cluster:admin/opendistro/ism/*",
            "cluster:admin/opendistro/rollup/*",
            "cluster:admin/opendistro/transform/*",
        )
        val indexPermissions = listOf(
            "indices:admin/opensearch/ism/*",
            "indices:admin/mappings/get",
            "indices:data/read/search"
        )
        // Create role - "finance_im_role"
        addRole("finance_im_role", clusterPermissions, listOf("finance-*"), indexPermissions)

        // Create role - "hr_im_role"
        addRole("hr_im_role", clusterPermissions, listOf("hr-*"), indexPermissions)

        // add roles to all the users
        addUsersToRole("finance_im_role", listOf("jane"))
        addUsersToRole("hr_im_role", listOf("jack"))
        addUsersToRole("all_access", listOf("sam"))

        financeUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), "jane", "Test123!").setSocketTimeout(60000).build()
        hrUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), "jack", "Test123!").setSocketTimeout(60000).build()
        adminUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), "sam", "Test123!").setSocketTimeout(60000).build()
        noAuthUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), "noauth", "Test123!").setSocketTimeout(60000).build()
    }
}
