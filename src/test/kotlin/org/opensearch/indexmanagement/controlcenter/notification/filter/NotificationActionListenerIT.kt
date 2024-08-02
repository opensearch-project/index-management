/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter

import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.opensearch.action.admin.indices.open.OpenIndexAction
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.reindex.ReindexAction
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementRestTestCase
import org.opensearch.indexmanagement.controlcenter.notification.util.supportedActions
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import java.net.InetAddress
import java.net.InetSocketAddress
import java.time.Instant

class NotificationActionListenerIT : IndexManagementRestTestCase() {
    private val notificationConfId = "test-notification-id"
    private val notificationIndex = "test-notification-index"

    private lateinit var server: HttpServer
    private lateinit var client: RestClient

    @Before
    fun startMockWebHook() {
        server = HttpServer.create(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        logger.info("starting mock server at {}", server.address.hostString)

        val httpHandler =
            HttpHandler {
                val msg = String(it.requestBody.readAllBytes())
                logger.info(msg)
                val res =
                    client.makeRequest(
                        "POST", "$notificationIndex/_doc?refresh=true",
                        StringEntity("""{"msg": "${msg.replace(System.lineSeparator(), " ")}"}""", ContentType.APPLICATION_JSON),
                    )
                logger.info(res.restStatus())

                it.sendResponseHeaders(200, "ack".toByteArray().size.toLong())
                it.responseBody.write("ack".toByteArray())
                it.close()
            }

        server.createContext("/notification", httpHandler)
        server.createContext("/notification2", httpHandler)
        server.start()

        setNotificationChannel()
    }

    @After
    fun stopMockServerAndClean() {
        server.stop(10)
        wipeAllIndices()
    }

    private fun setNotificationChannel() {
        client = client()
        if (indexExists(notificationIndex)) {
            deleteIndex(notificationIndex)
        }
        createIndex(notificationIndex, Settings.EMPTY)
        client.makeRequest(
            "POST", "/_plugins/_notifications/configs",
            StringEntity(
                """
                {
                  "config_id": "$notificationConfId",
                  "name": "test-webhook",
                  "config": {
                    "name": "Sample webhook Channel",
                    "description": "This is a webhook channel",
                    "config_type": "webhook",
                    "is_enabled": true,
                    "webhook": {
                      "url": "http://${server.address.hostString}:${server.address.port}/notification"
                    }
                  }
                }
                """.trimIndent(),
                ContentType.APPLICATION_JSON,
            ),
        )

        supportedActions.forEach { action ->
            client.makeRequest(
                "POST", "_plugins/_im/lron",
                StringEntity(
                    """
                    {
                        "lron_config": {
                            "action_name": "$action",
                            "channels": [
                                {
                                    "id": "$notificationConfId"
                                }
                            ]
                        }
                    }
                    """.trimIndent(),
                    ContentType.APPLICATION_JSON,
                ),
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notify for force merge`() {
        insertSampleData("source-index", 10)

        val response =
            client.makeRequest(
                "POST", "source-index/_forcemerge",
            )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                1,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:merge")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"],
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notify for resize`() {
        insertSampleData("source-index", 10)
        updateIndexSettings("source-index", Settings.builder().put("index.blocks.write", true))

        val response =
            client.makeRequest(
                "POST", "source-index/_split/test-split",
                StringEntity(
                    """
                    {
                        "settings":{
                            "index.number_of_shards": 2
                        }
                    }
                    """.trimIndent(),
                    ContentType.APPLICATION_JSON,
                ),
            )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                1,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:Split")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"],
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notify for open`() {
        insertSampleData("source-index", 10)

        closeIndex("source-index")

        val response =
            client.makeRequest(
                "POST", "source-index/_open",
            )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                1,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:Open")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"],
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notify for reindex`() {
        val response = performReindex()

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                1,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:reindex")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"],
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notify for reindex with runtime policy`() {
        insertSampleData("source-index", 10)
        createIndex("reindex-dest", Settings.EMPTY)
        client.makeRequest(
            "POST", "/_plugins/_notifications/configs",
            StringEntity(
                """
                {
                  "config_id": "config_id",
                  "name": "test-webhook2",
                  "config": {
                    "name": "Sample webhook Channel2",
                    "description": "This is a webhook channel2",
                    "config_type": "webhook",
                    "is_enabled": true,
                    "webhook": {
                      "url": "http://${server.address.hostString}:${server.address.port}/notification2"
                    }
                  }
                }
                """.trimIndent(),
                ContentType.APPLICATION_JSON,
            ),
        )

        val response =
            client.makeRequest(
                "POST", "_reindex?wait_for_completion=false",
                StringEntity(
                    """
                    {
                      "source": {
                        "index": "source-index"
                      },
                      "dest": {
                        "index": "reindex-dest"
                      }
                    }
                    """.trimIndent(),
                    ContentType.APPLICATION_JSON,
                ),
            )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)
        val taskId = response.asMap()["task"] as String
        logger.info("task id {}", taskId)

        val policyResponse =
            client.makeRequest(
                "POST", "_plugins/_im/lron",
                StringEntity(
                    """
                    {
                        "lron_config": {
                            "action_name": "${ReindexAction.NAME}",
                            "task_id": "$taskId",
                            "channels": [
                                {
                                    "id": "config_id"
                                }
                            ]
                        }
                    }
                    """.trimIndent(),
                    ContentType.APPLICATION_JSON,
                ),
            )
        val id = policyResponse.asMap()["_id"] as String
        logger.info("policy id {}", id)

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                2,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:Reindex")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"],
            )

            // runtime policy been removed
            val res =
                try {
                    client.makeRequest("GET", "_plugins/_im/lron/${id.replace("/", "%2F")}")
                } catch (e: ResponseException) {
                    e.response
                }
            assertEquals(RestStatus.NOT_FOUND.status, res.statusLine.statusCode)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notify for reindex with duplicate channel`() {
        val response = performReindex()

        Assert.assertTrue(response.restStatus() == RestStatus.OK)
        val taskId = response.asMap()["task"] as String

        client.makeRequest(
            "POST", "_plugins/_im/lron",
            StringEntity(
                """
                {
                    "lron_config": {
                        "action_name": "${ReindexAction.NAME}",
                        "task_id": "$taskId",
                        "channels": [
                            {
                                "id": "$notificationConfId"
                            }
                        ]
                    }
                }
                """.trimIndent(),
                ContentType.APPLICATION_JSON,
            ),
        )

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                1,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:Reindex")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"],
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test no notification for close`() {
        createIndex("test-index-create", Settings.EMPTY)
        closeIndex("test-index-create")

        waitFor {
            assertEquals(
                "Notification index have a doc for close",
                0,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:Close")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"],
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test no notification policy is configured`() {
        insertSampleData("source-index", 10)
        closeIndex("source-index")

        // remove notification policy
        client.makeRequest("DELETE", "_plugins/_im/lron/LRON:${OpenIndexAction.NAME.replace("/", "%2F")}")

        val response =
            client.makeRequest(
                "POST", "source-index/_open",
            )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        // should not have notification send out
        waitFor {
            assertEquals(
                "Notification index does not have a doc",
                0,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:Open")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"],
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notification policy system index is not exists`() {
        insertSampleData("source-index", 10)
        closeIndex("source-index")

        // delete system index
        adminClient().makeRequest("DELETE", IndexManagementPlugin.CONTROL_CENTER_INDEX)

        val response =
            client.makeRequest(
                "POST", "source-index/_open",
            )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        // should not have notification send out
        waitFor {
            assertEquals(
                "Notification index does not have a doc",
                0,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:Open")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"],
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test runtime policy been removed when index operation finished`() {
        val response = performReindex()

        Assert.assertTrue(response.restStatus() == RestStatus.OK)
        val taskId = response.asMap()["task"]
        Assert.assertNotNull(taskId)

        // put runtime policy only for failure
        client.makeRequest(
            "POST", "_plugins/_im/lron",
            StringEntity(
                """
                {
                    "lron_config": {
                        "task_id": "$taskId",
                        "lron_condition": {
                            "failure": true,
                            "success": false
                        },
                        "channels": [
                            {
                                "id": "$notificationConfId"
                            }
                        ]
                    }
                }
                """.trimIndent(),
                ContentType.APPLICATION_JSON,
            ),
        )

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                1,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:reindex")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"],
            )

            try {
                client.makeRequest("GET", "_plugins/_im/lron/LRON:$taskId")
            } catch (e: ResponseException) {
                // runtime policy been removed
                Assert.assertTrue(e.response.restStatus() == RestStatus.NOT_FOUND)
            }
        }
    }

    private fun performReindex(): Response {
        insertSampleData("source-index", 10)
        createIndex("reindex-dest", Settings.EMPTY)

        val response =
            client.makeRequest(
                "POST", "_reindex?wait_for_completion=false",
                StringEntity(
                    """
                    {
                      "source": {
                        "index": "source-index"
                      },
                      "dest": {
                        "index": "reindex-dest"
                      }
                    }
                    """.trimIndent(),
                    ContentType.APPLICATION_JSON,
                ),
            )
        return response
    }
}
