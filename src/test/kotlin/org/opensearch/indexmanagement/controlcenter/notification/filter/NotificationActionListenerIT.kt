/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter

import com.sun.net.httpserver.HttpServer
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.opensearch.client.RestClient
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementRestTestCase
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestStatus
import java.net.BindException
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
        server = try {
            HttpServer.create(InetSocketAddress(InetAddress.getLocalHost(), 0), 0)
        } catch (e: BindException) {
            HttpServer.create(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        }
        logger.info("starting mock server at {}", server.address.hostString)

        server.createContext("/notification") {
            val msg = String(it.requestBody.readAllBytes())
            logger.info(msg)
            val res = client.makeRequest(
                "POST", "$notificationIndex/_doc?refresh=true",
                StringEntity("""{"msg": "${msg.replace(System.lineSeparator(), " ")}"}""", ContentType.APPLICATION_JSON)
            )
            logger.info(res.restStatus())

            it.sendResponseHeaders(200, "ack".toByteArray().size.toLong())
            it.responseBody.write("ack".toByteArray())
        }
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
                ContentType.APPLICATION_JSON
            )
        )

        client.makeRequest(
            "POST", "_plugins/_im/lron",
            StringEntity(
                """
                {
                    "lron_config": {
                        "channels": [
                            {
                                "id": "$notificationConfId"
                            }
                        ]
                    }
                }
                """.trimIndent(),
                ContentType.APPLICATION_JSON
            )
        )
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notify for force merge`() {
        insertSampleData("source-index", 10)

        val response = client.makeRequest(
            "POST", "source-index/_forcemerge"
        )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                1,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:force_merge")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"]
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notify for resize`() {
        insertSampleData("source-index", 10)
        updateIndexSettings("source-index", Settings.builder().put("index.blocks.write", true))

        val response = client.makeRequest(
            "POST", "source-index/_split/test-split",
            StringEntity(
                """
                {
                    "settings":{
                        "index.number_of_shards": 2
                    }
                }
                """.trimIndent(),
                ContentType.APPLICATION_JSON
            )
        )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                1,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:split")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"]
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notify for open`() {
        insertSampleData("source-index", 10)

        closeIndex("source-index")

        val response = client.makeRequest(
            "POST", "source-index/_open"
        )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                1,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:open")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"]
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notify for reindex`() {
        insertSampleData("source-index", 10)
        createIndex("reindex-dest", Settings.EMPTY)

        val response = client.makeRequest(
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
                ContentType.APPLICATION_JSON
            )
        )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        waitFor(Instant.ofEpochSecond(60)) {
            assertEquals(
                "Notification index does not have a doc",
                1,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:reindex")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"]
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
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:close")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"]
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test no notification policy is configured`() {
        insertSampleData("source-index", 10)
        closeIndex("source-index")

        // remove notification policy
        client.makeRequest("DELETE", "_plugins/_im/lron/LRON:default")

        val response = client.makeRequest(
            "POST", "source-index/_open"
        )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        // should not have notification send out
        waitFor {
            assertEquals(
                "Notification index does not have a doc",
                0,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:open")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"]
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test notification policy system index is not exists`() {
        insertSampleData("source-index", 10)
        closeIndex("source-index")

        // delete system index
        client.makeRequest("DELETE", IndexManagementPlugin.CONTROL_CENTER_INDEX)

        val response = client.makeRequest(
            "POST", "source-index/_open"
        )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        // should not have notification send out
        waitFor {
            assertEquals(
                "Notification index does not have a doc",
                0,
                (
                    client.makeRequest("GET", "$notificationIndex/_search?q=msg:open")
                        .asMap() as Map<String, Map<String, Map<String, Any>>>
                    )["hits"]!!["total"]!!["value"]
            )
        }
    }
}
