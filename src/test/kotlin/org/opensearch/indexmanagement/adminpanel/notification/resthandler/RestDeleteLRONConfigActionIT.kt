/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.resthandler

import org.junit.Assert
import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.adminpanel.notification.getResourceURI
import org.opensearch.indexmanagement.adminpanel.notification.randomLRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.util.getDocID
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.rest.RestStatus

@Suppress("UNCHECKED_CAST")
class RestDeleteLRONConfigActionIT : LRONConfigRestTestCase() {
    fun `test delete LRONConfig`() {
        val lronConfig = randomLRONConfig()
        createLRONConfig(lronConfig)

        val response = client().makeRequest("DELETE", getResourceURI(lronConfig.taskId, lronConfig.actionName))
        assertEquals("delete LRONConfig failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        val deletedId = responseBody["_id"] as String
        Assert.assertEquals("not same doc id", getDocID(lronConfig.taskId, lronConfig.actionName), deletedId)

        try {
            client().makeRequest("GET", getResourceURI(lronConfig.taskId, lronConfig.actionName))
            fail("Expected 404 NOT_FOUND")
        } catch (e: ResponseException) {
            logger.info(e.response.asMap())
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test delete nonexist LRONConfig fails`() {
        try {
            val lronConfig = randomLRONConfig()
            client().makeRequest("DELETE", getResourceURI(lronConfig.taskId, lronConfig.actionName))
            fail("Expected 404 NOT_FOUND")
        } catch (e: ResponseException) {
            logger.info(e.response.asMap())
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
