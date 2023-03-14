/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.resthandler

import org.junit.Assert
import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.randomLRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.util.getDocID
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.rest.RestStatus

@Suppress("UNCHECKED_CAST")
class RestIndexLRONConfigActionIT : LRONConfigRestTestCase() {
    fun `test creating LRONConfig`() {
        val lronConfig = randomLRONConfig()
        val response = createLRONConfig(lronConfig)
        assertEquals("Create LRONConfig failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        val createdId = responseBody["_id"] as String
        Assert.assertEquals("not same doc id", getDocID(lronConfig.taskId, lronConfig.actionName), createdId)
        val lronConfigMap = lronConfig.convertToMap()[LRONConfig.LRON_CONFIG_FIELD] as Map<String, Any>
        Assert.assertEquals(
            "not same LRONConfig",
            lronConfigMap.filterKeys { it != LRONConfig.USER_FIELD && it != LRONConfig.PRIORITY_FIELD },
            responseBody["lron_config"] as Map<String, Any>
        )
    }

    fun `test creating LRONConfig with id fails`() {
        try {
            val lronConfig = randomLRONConfig()
            client().makeRequest(
                "POST",
                getResourceURI(lronConfig.taskId, lronConfig.actionName),
                emptyMap(),
                lronConfig.toHttpEntity()
            )
            fail("Expected 405 METHOD_NOT_ALLOWED")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }

    fun `test creating LRONConfig twice fails`() {
        try {
            val lronConfig = randomLRONConfig()
            createLRONConfig(lronConfig)
            createLRONConfig(lronConfig)
            fail("Expected 409 CONFLICT")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.CONFLICT, e.response.restStatus())
        }
    }

    fun `test mappings after LRONConfig creation`() {
        val lronConfig = randomLRONConfig()
        createLRONConfig(lronConfig)

        val response = client().makeRequest("GET", "/${IndexManagementPlugin.ADMIN_PANEL_INDEX}/_mapping")
        val parserMap = createParser(XContentType.JSON.xContent(), response.entity.content).map() as Map<String, Map<String, Any>>
        val mappingsMap = parserMap[IndexManagementPlugin.ADMIN_PANEL_INDEX]!!["mappings"] as Map<String, Any>
        val expected = createParser(
            XContentType.JSON.xContent(),
            javaClass.classLoader.getResource("mappings/opensearch-admin-panel.json")!!
                .readText()
        )
        val expectedMap = expected.map()

        assertEquals("Mappings are different", expectedMap, mappingsMap)
    }
}
