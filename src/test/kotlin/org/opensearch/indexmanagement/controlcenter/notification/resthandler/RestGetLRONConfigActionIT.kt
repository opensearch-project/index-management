/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.resthandler

import org.junit.Assert
import org.opensearch.client.ResponseException
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.controlcenter.notification.getResourceURI
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.nodeIdsInRestIT
import org.opensearch.indexmanagement.controlcenter.notification.randomLRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.randomTaskId
import org.opensearch.indexmanagement.controlcenter.notification.util.getDocID
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.test.OpenSearchTestCase

@Suppress("UNCHECKED_CAST")
class RestGetLRONConfigActionIT : LRONConfigRestTestCase() {
    fun `test get LRONConfig with id`() {
        val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
        createLRONConfig(lronConfig)
        val response = client().makeRequest("GET", getResourceURI(lronConfig.taskId, lronConfig.actionName))
        assertEquals("get LRONConfig failed", RestStatus.OK, response.restStatus())
        val lronConfigs = response.asMap()["lron_configs"] as List<Map<String, Any?>>
        val responseBody = lronConfigs[0]
        val gotId = responseBody["_id"] as String
        Assert.assertEquals("not same doc id", getDocID(lronConfig.taskId, lronConfig.actionName), gotId)
        val lronConfigMap = lronConfig.convertToMap()[LRONConfig.LRON_CONFIG_FIELD] as Map<String, Any>
        Assert.assertEquals(
            "not same LRONConfig",
            lronConfigMap.filterKeys { it != LRONConfig.USER_FIELD && it != LRONConfig.PRIORITY_FIELD },
            responseBody["lron_config"] as Map<String, Any>,
        )
    }

    fun `test get nonexist LRONConfig fails`() {
        // index a random doc to create .opensearch-control-center index
        createLRONConfig(randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random())))
        try {
            val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
            client().makeRequest("GET", getResourceURI(lronConfig.taskId, lronConfig.actionName))
            fail("Expected 404 NOT_FOUND")
        } catch (e: ResponseException) {
            logger.debug(e.response.asMap())
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test get all LRONConfigs`() {
        // LRONConfigRestTestCase index a doc to auto create the index, here we wipe the index before count doc number
        removeControlCenterIndex()
        val lronConfigResponses =
            randomList(1, 15) {
                createLRONConfig(randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))).asMap()
            }
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
                resLRONConfigResponse!![LRONConfig.LRON_CONFIG_FIELD],
            )
        }
    }

    fun `test get LRONConfig with docId and searchParams`() {
        try {
            val lronConfig = randomLRONConfig()
            client().makeRequest(
                "GET",
                getResourceURI(lronConfig.taskId, lronConfig.actionName),
                mapOf("size" to "10"),
            )
            Assert.fail("Expected 400 BAD_REQUEST")
        } catch (e: ResponseException) {
            logger.debug(e.response.asMap())
            assertEquals("unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test get all LRONConfig if index not exists`() {
        removeControlCenterIndex()
        val response = client().makeRequest("GET", IndexManagementPlugin.LRON_BASE_URI)
        assertEquals("get LRONConfigs failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        val totalNumber = responseBody["total_number"]
        OpenSearchTestCase.assertEquals("wrong LRONConfigs number", 0, totalNumber)
    }
}
