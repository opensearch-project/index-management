/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.client.ResponseException
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.TransitionsAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Conditions
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.IndexSimulateResult.Companion.CURRENT_ACTION_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.IndexSimulateResult.Companion.CURRENT_STATE_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.IndexSimulateResult.Companion.ERROR_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.IndexSimulateResult.Companion.INDEX_NAME_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.IndexSimulateResult.Companion.INDEX_UUID_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.IndexSimulateResult.Companion.IS_MANAGED_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.IndexSimulateResult.Companion.NEXT_STATE_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.IndexSimulateResult.Companion.TRANSITION_EVALUATION_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.SimulatePolicyResponse.Companion.SIMULATE_RESULTS_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.TransitionSimulateResult.Companion.CONDITION_MET_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.TransitionSimulateResult.Companion.CONDITION_TYPE_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate.TransitionSimulateResult.Companion.STATE_NAME_FIELD
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest.Method.POST
import java.util.Locale

@Suppress("UNCHECKED_CAST")
class RestSimulatePolicyActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    private val simulateUri = RestSimulatePolicyAction.SIMULATE_BASE_URI
    private val legacySimulateUri = RestSimulatePolicyAction.LEGACY_SIMULATE_BASE_URI

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private fun simulate(body: String): Map<String, Any> {
        val response = client().makeRequest(POST.toString(), simulateUri, StringEntity(body, ContentType.APPLICATION_JSON))
        assertEquals("Unexpected status", RestStatus.OK, response.restStatus())
        return response.asMap()
    }

    private fun simulateResults(body: String): List<Map<String, Any>> =
        simulate(body)[SIMULATE_RESULTS_FIELD] as List<Map<String, Any>>

    private fun singleResult(body: String): Map<String, Any> = simulateResults(body).single()

    // -------------------------------------------------------------------------
    // Stored policy — unmanaged index
    // -------------------------------------------------------------------------

    fun `test simulate stored policy on unmanaged index returns first state and action`() {
        val hotState = randomState(name = "hot", actions = listOf(), transitions = listOf())
        val policy = createPolicy(randomPolicy(states = listOf(hotState), ismTemplate = null))
        createIndex("$testIndexName-unmanaged", null)

        val result = singleResult(
            """
            {
              "policy_id": "${policy.id}",
              "indices": ["$testIndexName-unmanaged"]
            }
            """.trimIndent(),
        )

        assertEquals("$testIndexName-unmanaged", result[INDEX_NAME_FIELD])
        assertNotNull("index_uuid should be present", result[INDEX_UUID_FIELD])
        assertEquals(false, result[IS_MANAGED_FIELD])
        assertEquals("hot", result[CURRENT_STATE_FIELD])
        // The only action in an empty-action state is "transitions"
        assertEquals(TransitionsAction.name, result[CURRENT_ACTION_FIELD])
        assertNull("error should be null", result[ERROR_FIELD])
    }

    // -------------------------------------------------------------------------
    // Inline policy
    // -------------------------------------------------------------------------

    fun `test simulate inline policy without stored policy`() {
        createIndex("$testIndexName-inline", null)

        val result = singleResult(
            """
            {
              "policy": {
                "description": "inline test",
                "default_state": "warm",
                "states": [{"name":"warm","actions":[],"transitions":[]}]
              },
              "indices": ["$testIndexName-inline"]
            }
            """.trimIndent(),
        )

        assertEquals("$testIndexName-inline", result[INDEX_NAME_FIELD])
        assertEquals(false, result[IS_MANAGED_FIELD])
        assertEquals("warm", result[CURRENT_STATE_FIELD])
        assertNull("error should be null", result[ERROR_FIELD])
    }

    // -------------------------------------------------------------------------
    // Error cases
    // -------------------------------------------------------------------------

    fun `test simulate non-existent policy returns 404`() {
        createIndex("$testIndexName-err", null)
        try {
            simulate("""{"policy_id":"nonexistent-policy-xyz","indices":["$testIndexName-err"]}""")
            fail("Expected 404")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test simulate non-existent index returns error in result`() {
        val policy = createPolicy(randomPolicy(states = listOf(randomState(transitions = listOf()))))

        val result = singleResult(
            """{"policy_id":"${policy.id}","indices":["index-that-does-not-exist"]}""",
        )

        assertEquals("index-that-does-not-exist", result[INDEX_NAME_FIELD])
        assertNull("index_uuid should be null when index is not found", result[INDEX_UUID_FIELD])
        assertNotNull("error should be present for missing index", result[ERROR_FIELD])
        assertTrue((result[ERROR_FIELD] as String).contains("not found"))
    }

    // -------------------------------------------------------------------------
    // Wildcard expansion
    // -------------------------------------------------------------------------

    fun `test simulate with wildcard expands to multiple indices`() {
        val prefix = "$testIndexName-wild"
        createIndex("$prefix-1", null)
        createIndex("$prefix-2", null)
        createIndex("$prefix-3", null)
        val policy = createPolicy(randomPolicy(states = listOf(randomState(transitions = listOf()))))

        val results = simulateResults(
            """{"policy_id":"${policy.id}","indices":["$prefix-*"]}""",
        )

        val returnedNames = results.map { it[INDEX_NAME_FIELD] as String }.toSet()
        assertTrue("$prefix-1 should be in results", returnedNames.contains("$prefix-1"))
        assertTrue("$prefix-2 should be in results", returnedNames.contains("$prefix-2"))
        assertTrue("$prefix-3 should be in results", returnedNames.contains("$prefix-3"))
    }

    // -------------------------------------------------------------------------
    // Transition evaluation — conditions not met
    // -------------------------------------------------------------------------

    fun `test simulate shows transition evaluation with index age not met`() {
        val warmState = randomState(name = "warm", actions = listOf(), transitions = listOf())
        val hotState = State(
            name = "hot",
            actions = listOf(),
            transitions = listOf(
                Transition(
                    stateName = "warm",
                    conditions = Conditions(indexAge = TimeValue.timeValueDays(100)),
                ),
            ),
        )
        val policy = createPolicy(randomPolicy(states = listOf(hotState, warmState), ismTemplate = null))
        createIndex("$testIndexName-age", null)

        val result = singleResult(
            """{"policy_id":"${policy.id}","indices":["$testIndexName-age"]}""",
        )

        assertEquals(false, result[IS_MANAGED_FIELD])
        assertEquals("hot", result[CURRENT_STATE_FIELD])
        assertEquals(TransitionsAction.name, result[CURRENT_ACTION_FIELD])

        val evaluations = result[TRANSITION_EVALUATION_FIELD] as List<Map<String, Any>>
        assertEquals(1, evaluations.size)
        val transition = evaluations[0]
        assertEquals("warm", transition[STATE_NAME_FIELD])
        assertEquals(false, transition[CONDITION_MET_FIELD])
        assertEquals(Conditions.MIN_INDEX_AGE_FIELD, transition[CONDITION_TYPE_FIELD])

        // next_state should be null since the condition is not met
        assertNull("next_state should be null", result[NEXT_STATE_FIELD])
    }

    fun `test simulate shows transition evaluation with doc count not met`() {
        val coldState = randomState(name = "cold", actions = listOf(), transitions = listOf())
        val hotState = State(
            name = "hot",
            actions = listOf(),
            transitions = listOf(
                Transition(
                    stateName = "cold",
                    conditions = Conditions(docCount = 1_000_000L),
                ),
            ),
        )
        val policy = createPolicy(randomPolicy(states = listOf(hotState, coldState), ismTemplate = null))
        createIndex("$testIndexName-docs", null)

        val result = singleResult(
            """{"policy_id":"${policy.id}","indices":["$testIndexName-docs"]}""",
        )

        val evaluations = result[TRANSITION_EVALUATION_FIELD] as List<Map<String, Any>>
        assertEquals(1, evaluations.size)
        assertEquals(false, evaluations[0][CONDITION_MET_FIELD])
        assertEquals(Conditions.MIN_DOC_COUNT_FIELD, evaluations[0][CONDITION_TYPE_FIELD])
        assertNull("next_state should be null", result[NEXT_STATE_FIELD])
    }

    fun `test simulate unconditional transition shows condition met`() {
        val warmState = randomState(name = "warm", actions = listOf(), transitions = listOf())
        // Transition with no conditions → unconditional, always moves
        val hotState = State(
            name = "hot",
            actions = listOf(),
            transitions = listOf(Transition(stateName = "warm", conditions = null)),
        )
        val policy = createPolicy(randomPolicy(states = listOf(hotState, warmState), ismTemplate = null))
        createIndex("$testIndexName-unconditional", null)

        val result = singleResult(
            """{"policy_id":"${policy.id}","indices":["$testIndexName-unconditional"]}""",
        )

        val evaluations = result[TRANSITION_EVALUATION_FIELD] as List<Map<String, Any>>
        assertEquals(1, evaluations.size)
        assertEquals(true, evaluations[0][CONDITION_MET_FIELD])
        assertEquals("warm", result[NEXT_STATE_FIELD])
    }

    // -------------------------------------------------------------------------
    // Managed index — is_managed=true
    // -------------------------------------------------------------------------

    fun `test simulate shows is_managed true for a managed index after initialization`() {
        val hotState = randomState(name = "hot", actions = listOf(), transitions = listOf())
        val policy = createPolicy(randomPolicy(states = listOf(hotState), ismTemplate = null))
        createIndex("$testIndexName-managed", policy.id)

        // Trigger ISM to initialize the managed index metadata
        val managedIndex = getExistingManagedIndexConfig("$testIndexName-managed")
        updateManagedIndexConfigStartTime(managedIndex)

        // Wait until ISM has written the metadata document
        waitFor {
            val result = singleResult(
                """{"policy_id":"${policy.id}","indices":["$testIndexName-managed"]}""",
            )
            assertTrue("Expected is_managed=true after ISM initialization", result[IS_MANAGED_FIELD] == true)
        }
    }

    // -------------------------------------------------------------------------
    // Legacy URI
    // -------------------------------------------------------------------------

    fun `test simulate works via legacy opendistro uri`() {
        createIndex("$testIndexName-legacy", null)
        val policy = createPolicy(randomPolicy(states = listOf(randomState(transitions = listOf()))))

        val response = client().makeRequest(
            POST.toString(), legacySimulateUri,
            StringEntity(
                """{"policy_id":"${policy.id}","indices":["$testIndexName-legacy"]}""",
                ContentType.APPLICATION_JSON,
            ),
        )
        assertEquals(RestStatus.OK, response.restStatus())
        val results = response.asMap()[SIMULATE_RESULTS_FIELD] as List<*>
        assertEquals(1, results.size)
    }

    // -------------------------------------------------------------------------
    // Multiple indices in one request
    // -------------------------------------------------------------------------

    fun `test simulate multiple concrete indices returns one result per index`() {
        createIndex("$testIndexName-multi-1", null)
        createIndex("$testIndexName-multi-2", null)
        val policy = createPolicy(randomPolicy(states = listOf(randomState(transitions = listOf()))))

        val results = simulateResults(
            """{"policy_id":"${policy.id}","indices":["$testIndexName-multi-1","$testIndexName-multi-2"]}""",
        )

        assertEquals(2, results.size)
        val names = results.map { it[INDEX_NAME_FIELD] as String }.toSet()
        assertTrue(names.contains("$testIndexName-multi-1"))
        assertTrue(names.contains("$testIndexName-multi-2"))
    }
}
