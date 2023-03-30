/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.resthandler

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.HttpEntity
import org.apache.hc.core5.http.io.entity.StringEntity
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementRestTestCase
import org.opensearch.indexmanagement.controlcenter.notification.initNodeIdsInRestIT
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.toJsonString
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.rest.RestStatus

abstract class LRONConfigRestTestCase : IndexManagementRestTestCase() {
    @Before
    fun setDebugLogLevel() {
        client().makeRequest(
            "PUT", "_cluster/settings",
            StringEntity(
                """
                {
                    "transient": {
                        "logger.org.opensearch.indexmanagement.controlcenter.notification":"DEBUG"
                    }
                }
                """.trimIndent(),
                ContentType.APPLICATION_JSON
            )
        )
    }

    @Before
    fun initNodeIds() {
        initNodeIdsInRestIT(client())
    }

    @After
    fun removeAllDocs() {
        try {
            client().makeRequest(
                "POST",
                "${IndexManagementPlugin.CONTROL_CENTER_INDEX}/_delete_by_query",
                mapOf("refresh" to "true"),
                StringEntity("""{"query": {"match_all": {}}}""", ContentType.APPLICATION_JSON)
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
        @JvmStatic fun clearIndicesAfterClass() {
            wipeAllIndices()
        }
    }
}
