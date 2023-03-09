/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filters

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.junit.Assert
import org.junit.Before
import org.opensearch.indexmanagement.IndexManagementRestTestCase
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.rest.RestStatus

class NotificationActionListenerIT : IndexManagementRestTestCase() {

    private val notificationConfId = "test-notification-id"
    private val notificationIndex = "test-notification-index"

    @Before
    fun setup() {
        val clusterUri = System.getProperty("tests.rest.cluster").split(",")[0]

        client().makeRequest(
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
                          "url": "$protocol://$clusterUri/$notificationIndex/_doc"
                        }
                      }
                    }
                """.trimIndent(),
                ContentType.APPLICATION_JSON
            )
        )

        client().makeRequest(
            "PUT", "_plugins/_im/lron",
            StringEntity(
                """
                {
                    "lron_config": {
                        "action_name": "indices:admin/forcemerge",
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
    fun `test notify for reindex`() {
        insertSampleData("source-index", 10)

        val response = client().makeRequest(
            "POST", "source-index/_forcemerge"
        )

        Assert.assertTrue(response.restStatus() == RestStatus.OK)

        // TODO notification plugin custom webhook is broken
//        waitFor {
//            assertEquals(
//                "Notification index does not have a doc",
//                1,
//                (
//                    client().makeRequest("GET", "$notificationIndex/_search")
//                        .asMap() as Map<String, Map<String, Map<String, Any>>>
//                    )["hits"]!!["total"]!!["value"]
//            )
//        }
    }
}
