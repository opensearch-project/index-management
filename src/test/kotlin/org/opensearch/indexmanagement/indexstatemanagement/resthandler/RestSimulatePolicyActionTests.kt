/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import com.nhaarman.mockitokotlin2.mock
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.bytes.BytesArray
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.SimulatePolicyRequest
import org.opensearch.rest.RestRequest
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.rest.FakeRestChannel
import org.opensearch.test.rest.FakeRestRequest
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.node.NodeClient

class RestSimulatePolicyActionTests : OpenSearchTestCase() {
    private val handler = RestSimulatePolicyAction()

    /**
     * Minimal NodeClient subclass that captures whatever request is dispatched via
     * execute().  AbstractClient.execute() is final and delegates to doExecute(), so
     * overriding doExecute() lets us intercept without running the full transport stack.
     */
    private inner class CapturingClient : NodeClient(Settings.EMPTY, mock<ThreadPool>()) {
        var captured: SimulatePolicyRequest? = null

        override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
            action: ActionType<Response>,
            request: Request,
            listener: ActionListener<Response>,
        ) {
            captured = request as? SimulatePolicyRequest
        }
    }

    private fun buildRestRequest(json: String): RestRequest =
        FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.POST)
            .withPath(RestSimulatePolicyAction.SIMULATE_BASE_URI)
            .withContent(BytesArray(json), XContentType.JSON)
            .build()

    /**
     * Calls handleRequest() (the public entry point on BaseRestHandler), which
     * internally calls prepareRequest() then consumer.accept(channel).  The
     * CapturingClient intercepts the final execute() call and stores the request.
     */
    private fun parseRequest(json: String): SimulatePolicyRequest {
        val client = CapturingClient()
        val restRequest = buildRestRequest(json)
        handler.handleRequest(restRequest, FakeRestChannel(restRequest, false, 1), client)
        return requireNotNull(client.captured) { "doExecute was not called — request was not dispatched" }
    }

    // -------------------------------------------------------------------------
    // Handler metadata
    // -------------------------------------------------------------------------

    fun `test getName returns correct action name`() {
        assertEquals("ism_simulate_policy_action", handler.getName())
    }

    fun `test routes returns empty list`() {
        assertTrue(handler.routes().isEmpty())
    }

    fun `test replacedRoutes includes simulate base uri as POST`() {
        val routes = handler.replacedRoutes()
        assertTrue(routes.any { it.method == RestRequest.Method.POST && it.path == RestSimulatePolicyAction.SIMULATE_BASE_URI })
    }

    fun `test replacedRoutes includes legacy simulate base uri as POST`() {
        val routes = handler.replacedRoutes()
        // The legacy URI is the deprecated half of the ReplacedRoute
        assertTrue(
            routes.any {
                it.deprecatedMethod == RestRequest.Method.POST &&
                    it.deprecatedPath == RestSimulatePolicyAction.LEGACY_SIMULATE_BASE_URI
            },
        )
    }

    // -------------------------------------------------------------------------
    // prepareRequest — field parsing verification
    // -------------------------------------------------------------------------

    fun `test prepareRequest parses policy_id and indices`() {
        val req = parseRequest("""{"policy_id":"my-policy","indices":["idx-1","idx-2"]}""")

        assertEquals("my-policy", req.policyId)
        assertNull(req.policy)
        assertEquals(listOf("idx-1", "idx-2"), req.indices)
    }

    fun `test prepareRequest parses inline policy`() {
        val json =
            """
            {
              "policy": {
                "description": "test policy",
                "default_state": "hot",
                "states": [{"name":"hot","actions":[],"transitions":[]}]
              },
              "indices": ["test-index"]
            }
            """.trimIndent()

        val req = parseRequest(json)

        assertNull(req.policyId)
        assertNotNull(req.policy)
        assertEquals("hot", req.policy!!.defaultState)
        assertEquals(listOf("test-index"), req.indices)
    }

    fun `test prepareRequest parses multiple indices`() {
        val req = parseRequest("""{"policy_id":"p","indices":["a","b","c"]}""")

        assertEquals(listOf("a", "b", "c"), req.indices)
    }

    fun `test prepareRequest parses empty indices array`() {
        val req = parseRequest("""{"policy_id":"p","indices":[]}""")

        assertTrue(req.indices.isEmpty())
        assertEquals("p", req.policyId)
    }

    fun `test prepareRequest skips unknown fields`() {
        val req = parseRequest(
            """{"policy_id":"my-policy","indices":["i"],"unknown_field":"value","nested":{"a":1}}""",
        )

        assertEquals("my-policy", req.policyId)
        assertEquals(listOf("i"), req.indices)
    }

    fun `test prepareRequest empty body produces null policy_id and empty indices`() {
        val req = parseRequest("""{}""")

        assertNull(req.policyId)
        assertNull(req.policy)
        assertTrue(req.indices.isEmpty())
    }
}
