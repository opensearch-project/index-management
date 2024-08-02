/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.resthandler

import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementRestTestCase
import org.opensearch.indexmanagement.controlcenter.notification.initNodeIdsInRestIT
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.toJsonString
import org.opensearch.indexmanagement.makeRequest

abstract class LRONConfigRestTestCase : IndexManagementRestTestCase() {
    @Before
    fun prepareForIT() {
        /* init cluster node ids in integ test */
        initNodeIdsInRestIT(client())
    }

    @After
    fun removeAllDocs() {
        try {
            adminClient().makeRequest(
                "POST",
                "${IndexManagementPlugin.CONTROL_CENTER_INDEX}/_delete_by_query",
                mapOf("refresh" to "true"),
                StringEntity("""{"query": {"match_all": {}}}""", ContentType.APPLICATION_JSON),
            )
        } catch (e: ResponseException) {
            logger.info(e.response.asMap())
            /* ignore if the index has not been created */
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun createLRONConfig(lronConfig: LRONConfig): Response {
        return client().makeRequest("POST", IndexManagementPlugin.LRON_BASE_URI, emptyMap(), lronConfig.toHttpEntity())
    }

    protected fun LRONConfig.toHttpEntity(): HttpEntity = StringEntity(toJsonString(), ContentType.APPLICATION_JSON)

    companion object {
        @AfterClass
        @JvmStatic fun removeControlCenterIndex() {
            try {
                adminClient().makeRequest("DELETE", IndexManagementPlugin.CONTROL_CENTER_INDEX, emptyMap())
            } catch (e: ResponseException) {
                /* ignore if the index has not been created */
                assertEquals("Unexpected status", RestStatus.NOT_FOUND, RestStatus.fromCode(e.response.statusLine.statusCode))
            }
        }
    }
}
