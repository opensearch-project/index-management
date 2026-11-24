/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import com.nhaarman.mockitokotlin2.mock
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.executemanagedindex.ExecuteManagedIndexRequest
import org.opensearch.rest.RestRequest
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.rest.FakeRestChannel
import org.opensearch.test.rest.FakeRestRequest
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.node.NodeClient

class RestExecuteManagedIndexActionTests : OpenSearchTestCase() {
    private val handler = RestExecuteManagedIndexAction()

    /** Captures the request that is dispatched through execute() instead of running the transport stack. */
    private inner class CapturingClient : NodeClient(Settings.EMPTY, mock<ThreadPool>()) {
        var captured: ExecuteManagedIndexRequest? = null

        override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
            action: ActionType<Response>,
            request: Request,
            listener: ActionListener<Response>,
        ) {
            captured = request as? ExecuteManagedIndexRequest
        }
    }

    private fun restRequest(params: Map<String, String>): RestRequest = FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
        .withMethod(RestRequest.Method.POST)
        .withPath("${RestExecuteManagedIndexAction.EXECUTE_BASE_URI}/${params["index"] ?: ""}")
        .withParams(params)
        .build()

    private fun handle(params: Map<String, String>, client: CapturingClient) {
        val request = restRequest(params)
        handler.handleRequest(request, FakeRestChannel(request, false, 1), client)
    }

    fun `test the route is a post on the execute uri with an index`() {
        val routes = handler.routes()

        assertEquals(1, routes.size)
        assertEquals(RestRequest.Method.POST, routes[0].method)
        assertEquals("${RestExecuteManagedIndexAction.EXECUTE_BASE_URI}/{index}", routes[0].path)
    }

    fun `test the handler has a name`() {
        assertEquals("execute_managed_index", handler.name)
    }

    fun `test the index of the path goes into the request`() {
        val client = CapturingClient()

        handle(mapOf("index" to "my-index"), client)

        assertEquals("my-index", client.captured?.index)
    }

    fun `test a missing index is rejected`() {
        expectThrows(IllegalArgumentException::class.java) { handle(emptyMap(), CapturingClient()) }
    }

    fun `test a blank index is rejected`() {
        expectThrows(IllegalArgumentException::class.java) { handle(mapOf("index" to " "), CapturingClient()) }
    }
}
