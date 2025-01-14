/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.resthandler

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.controlcenter.notification.getResourceURI
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.nodeIdsInRestIT
import org.opensearch.indexmanagement.controlcenter.notification.randomLRONCondition
import org.opensearch.indexmanagement.controlcenter.notification.randomLRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.randomTaskId
import org.opensearch.indexmanagement.controlcenter.notification.util.getDocID
import org.opensearch.indexmanagement.indexstatemanagement.randomChannel
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.util.DRY_RUN
import java.util.concurrent.Executors

@Suppress("UNCHECKED_CAST")
class RestIndexLRONConfigActionIT : LRONConfigRestTestCase() {
    fun `test creating LRONConfig using POST`() {
        val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
        val response = createLRONConfig(lronConfig)
        assertEquals("Create LRONConfig failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        val createdId = responseBody["_id"] as String
        Assert.assertEquals("not same doc id", getDocID(lronConfig.taskId, lronConfig.actionName), createdId)
        val lronConfigMap = lronConfig.convertToMap()[LRONConfig.LRON_CONFIG_FIELD] as Map<String, Any>
        Assert.assertEquals(
            "not same LRONConfig",
            lronConfigMap.filterKeys { it != LRONConfig.USER_FIELD && it != LRONConfig.PRIORITY_FIELD },
            responseBody["lron_config"] as Map<String, Any>,
        )
    }

    fun `test creating LRONConfig with id fails using POST`() {
        try {
            val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
            client().makeRequest(
                "POST",
                getResourceURI(lronConfig.taskId, lronConfig.actionName),
                emptyMap(),
                lronConfig.toHttpEntity(),
            )
            fail("Expected 405 METHOD_NOT_ALLOWED")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }

    fun `test creating LRONConfig twice fails using POST`() {
        try {
            val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
            createLRONConfig(lronConfig)
            createLRONConfig(lronConfig)
            fail("Expected 409 CONFLICT")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.CONFLICT, e.response.restStatus())
        }
    }

    fun `test update LRONConfig using PUT`() {
        val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
        createLRONConfig(lronConfig)

        val newLRONConfig =
            LRONConfig(
                lronCondition = randomLRONCondition(),
                taskId = lronConfig.taskId,
                actionName = lronConfig.actionName,
                channels = List(10) { randomChannel() },
                user = null,
                priority = null,
            )

        val response =
            client().makeRequest(
                "PUT",
                getResourceURI(lronConfig.taskId, lronConfig.actionName),
                emptyMap(),
                newLRONConfig.toHttpEntity(),
            )

        assertEquals("update LRONConfig failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        val updatedId = responseBody["_id"] as String
        Assert.assertEquals("not same doc id", getDocID(lronConfig.taskId, lronConfig.actionName), updatedId)
        val newLRONConfigMap = newLRONConfig.convertToMap()[LRONConfig.LRON_CONFIG_FIELD] as Map<String, Any>
        Assert.assertEquals(
            "not same LRONConfig",
            newLRONConfigMap.filterKeys { it != LRONConfig.USER_FIELD && it != LRONConfig.PRIORITY_FIELD },
            responseBody["lron_config"] as Map<String, Any>,
        )
    }

    fun `test create LRONConfig using PUT`() {
        val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
        val response =
            client().makeRequest(
                "PUT",
                getResourceURI(lronConfig.taskId, lronConfig.actionName),
                emptyMap(),
                lronConfig.toHttpEntity(),
            )
        assertEquals("autocreate LRONConfig failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        val lronConfigMap = lronConfig.convertToMap()[LRONConfig.LRON_CONFIG_FIELD] as Map<String, Any>
        Assert.assertEquals(
            "not same LRONConfig",
            lronConfigMap.filterKeys { it != LRONConfig.USER_FIELD && it != LRONConfig.PRIORITY_FIELD },
            responseBody["lron_config"] as Map<String, Any>,
        )
    }

    fun `test creating LRONConfig without id fails using PUT`() {
        try {
            val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
            client().makeRequest(
                "PUT",
                IndexManagementPlugin.LRON_BASE_URI,
                emptyMap(),
                lronConfig.toHttpEntity(),
            )
            fail("Expected 405 METHOD_NOT_ALLOWED")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }

    fun `test creating LRONConfig dryRun`() {
        val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
        // first use POST and PUT to create, then try to get
        client().makeRequest(
            "POST",
            IndexManagementPlugin.LRON_BASE_URI,
            mapOf(DRY_RUN to "true"),
            lronConfig.toHttpEntity(),
        )
        client().makeRequest(
            "PUT",
            getResourceURI(lronConfig.taskId, lronConfig.actionName),
            mapOf(DRY_RUN to "true"),
            lronConfig.toHttpEntity(),
        )
        try {
            client().makeRequest("GET", getResourceURI(lronConfig.taskId, lronConfig.actionName))
            fail("Expected 404 NOT_FOUND")
        } catch (e: ResponseException) {
            logger.debug(e.response.asMap())
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test autocreate index for indexLRONConfig action`() {
        removeControlCenterIndex()
        val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
        var response = createLRONConfig(lronConfig)
        assertEquals("Create LRONConfig failed", RestStatus.OK, response.restStatus())
        removeControlCenterIndex()
        response =
            client().makeRequest(
                "PUT",
                getResourceURI(lronConfig.taskId, lronConfig.actionName),
                lronConfig.toHttpEntity(),
            )
        assertEquals("Create LRONConfig failed", RestStatus.OK, response.restStatus())
    }

    fun `test mappings after LRONConfig creation`() {
        removeControlCenterIndex()
        val lronConfig = randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random()))
        createLRONConfig(lronConfig)

        val response = adminClient().makeRequest("GET", "/${IndexManagementPlugin.CONTROL_CENTER_INDEX}/_mapping")
        val parserMap = createParser(XContentType.JSON.xContent(), response.entity.content).map() as Map<String, Map<String, Any>>
        val mappingsMap = parserMap[IndexManagementPlugin.CONTROL_CENTER_INDEX]!!["mappings"] as Map<String, Any>
        val expected =
            createParser(
                XContentType.JSON.xContent(),
                javaClass.classLoader.getResource("mappings/opensearch-control-center.json")!!
                    .readText(),
            )
        val expectedMap = expected.map()

        assertEquals("Mappings are different", expectedMap, mappingsMap)
    }

    fun `test concurrent indexing requests auto create index and not throw exception`() {
        removeControlCenterIndex()
        val threadSize = 5
        val lronConfigs = List(threadSize) { randomLRONConfig(taskId = randomTaskId(nodeId = nodeIdsInRestIT.random())) }
        val threadPool = Executors.newFixedThreadPool(threadSize)
        try {
            runBlocking {
                val dispatcher = threadPool.asCoroutineDispatcher()
                val responses =
                    lronConfigs.map {
                        async(dispatcher) {
                            createLRONConfig(it)
                        }
                    }.awaitAll()
                responses.forEach { assertEquals("Create LRONConfig failed", RestStatus.OK, it.restStatus()) }
            }
        } finally {
            threadPool.shutdown()
        }
        val response = client().makeRequest("GET", IndexManagementPlugin.LRON_BASE_URI)
        assertEquals("get LRONConfigs failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        val totalNumber = responseBody["total_number"]
        assertEquals("wrong LRONConfigs number", threadSize, totalNumber)
    }
}
