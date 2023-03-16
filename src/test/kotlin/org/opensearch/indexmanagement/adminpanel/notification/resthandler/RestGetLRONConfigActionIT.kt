/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.resthandler

import org.junit.Assert
import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.getResourceURI
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.randomLRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.util.getDocID
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.rest.RestStatus

@Suppress("UNCHECKED_CAST")
class RestGetLRONConfigActionIT : LRONConfigRestTestCase() {
    fun `test get LRONConfig`() {
        val lronConfig = randomLRONConfig()
        createLRONConfig(lronConfig)
        val response = client().makeRequest("GET", getResourceURI(lronConfig.taskId, lronConfig.actionName))
        assertEquals("get LRONConfig failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        val gotId = responseBody["_id"] as String
        Assert.assertEquals("not same doc id", getDocID(lronConfig.taskId, lronConfig.actionName), gotId)
        val lronConfigMap = lronConfig.convertToMap()[LRONConfig.LRON_CONFIG_FIELD] as Map<String, Any>
        Assert.assertEquals(
            "not same LRONConfig",
            lronConfigMap.filterKeys { it != LRONConfig.USER_FIELD && it != LRONConfig.PRIORITY_FIELD },
            responseBody["lron_config"] as Map<String, Any>
        )
    }

    fun `test get nonexist LRONConfig fails`() {
        try {
            val lronConfig = randomLRONConfig()
            client().makeRequest("GET", getResourceURI(lronConfig.taskId, lronConfig.actionName))
            fail("Expected 404 NOT_FOUND")
        } catch (e: ResponseException) {
            logger.info(e.response.asMap())
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test get all LRONConfigs`() {
        val lronConfigResponses = randomList(1, 15) { createLRONConfig(randomLRONConfig()).asMap() }

        val response = client().makeRequest("GET", IndexManagementPlugin.LRON_BASE_URI)
        assertEquals("get LRONConfigs failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        val totalNumber = responseBody["total_number"]
        val resLRONConfigResponses = responseBody["lron_configs"] as List<Map<String, Any?>>

        assertTrue("LRONConfigs total numbers was not the same", resLRONConfigResponses.size == totalNumber)
        assertTrue("LRONConfigs response has different size", lronConfigResponses.size == resLRONConfigResponses.size)

        for (lronConfigResponse in lronConfigResponses) {
            val resLRONConfigResponse = resLRONConfigResponses.find { lronConfigResponse["_id"] as String == it["_id"] as String }
            assertEquals(
                "different lronConfigResponse",
                lronConfigResponse[LRONConfig.LRON_CONFIG_FIELD],
                resLRONConfigResponse!![LRONConfig.LRON_CONFIG_FIELD]
            )
        }
    }
}
