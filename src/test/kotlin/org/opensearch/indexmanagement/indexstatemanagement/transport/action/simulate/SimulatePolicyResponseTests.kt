/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.test.OpenSearchTestCase

class SimulatePolicyResponseTests : OpenSearchTestCase() {
    // ------------------------------------------------------------------ //
    // TransitionSimulateResult                                             //
    // ------------------------------------------------------------------ //

    fun `test transition simulate result roundtrip with all fields`() {
        val result = TransitionSimulateResult(
            stateName = "warm",
            conditionMet = true,
            conditionType = "min_index_age",
            currentValue = "8d",
            requiredValue = "7d",
        )
        assertRoundTrip(result)
    }

    fun `test transition simulate result roundtrip with null optional fields`() {
        val result = TransitionSimulateResult(
            stateName = "warm",
            conditionMet = true,
            conditionType = "unconditional",
            currentValue = null,
            requiredValue = null,
        )
        assertRoundTrip(result)
    }

    fun `test transition simulate result condition not met`() {
        val result = TransitionSimulateResult(
            stateName = "cold",
            conditionMet = false,
            conditionType = "min_index_age",
            currentValue = "3d",
            requiredValue = "7d",
        )
        assertRoundTrip(result)
        assertFalse(result.conditionMet)
    }

    fun `test transition simulate result toXContent`() {
        val result = TransitionSimulateResult(
            stateName = "warm",
            conditionMet = true,
            conditionType = "min_index_age",
            currentValue = "8d",
            requiredValue = "7d",
        )
        val builder = XContentFactory.jsonBuilder()
        result.toXContent(builder, ToXContent.EMPTY_PARAMS)
        val json = builder.toString()
        assertTrue(json.contains("\"${TransitionSimulateResult.STATE_NAME_FIELD}\":\"warm\""))
        assertTrue(json.contains("\"${TransitionSimulateResult.CONDITION_MET_FIELD}\":true"))
        assertTrue(json.contains("\"${TransitionSimulateResult.CONDITION_TYPE_FIELD}\":\"min_index_age\""))
        assertTrue(json.contains("\"${TransitionSimulateResult.CURRENT_VALUE_FIELD}\":\"8d\""))
        assertTrue(json.contains("\"${TransitionSimulateResult.REQUIRED_VALUE_FIELD}\":\"7d\""))
    }

    fun `test transition simulate result toXContent omits null optional fields`() {
        val result = TransitionSimulateResult(
            stateName = "warm",
            conditionMet = true,
            conditionType = "unconditional",
            currentValue = null,
            requiredValue = null,
        )
        val builder = XContentFactory.jsonBuilder()
        result.toXContent(builder, ToXContent.EMPTY_PARAMS)
        val json = builder.toString()
        assertFalse("currentValue should be absent", json.contains(TransitionSimulateResult.CURRENT_VALUE_FIELD))
        assertFalse("requiredValue should be absent", json.contains(TransitionSimulateResult.REQUIRED_VALUE_FIELD))
    }

    // ------------------------------------------------------------------ //
    // IndexSimulateResult                                                  //
    // ------------------------------------------------------------------ //

    fun `test index simulate result roundtrip with all fields`() {
        val transitionResult = TransitionSimulateResult("warm", true, "min_index_age", "8d", "7d")
        val result = IndexSimulateResult(
            indexName = "my-index",
            indexUUID = "abc-uuid",
            policyId = "my-policy",
            isManaged = true,
            currentState = "hot",
            currentAction = "rollover",
            transitionEvaluation = listOf(transitionResult),
            nextState = "warm",
            error = null,
        )
        assertRoundTrip(result)
    }

    fun `test index simulate result roundtrip with error set`() {
        val result = IndexSimulateResult(
            indexName = "missing-index",
            indexUUID = null,
            policyId = "my-policy",
            isManaged = false,
            currentState = null,
            currentAction = null,
            transitionEvaluation = null,
            nextState = null,
            error = "Index 'missing-index' not found in cluster",
        )
        assertRoundTrip(result)
    }

    fun `test index simulate result roundtrip with null transition evaluation`() {
        val result = IndexSimulateResult(
            indexName = "my-index",
            indexUUID = "uuid-1",
            policyId = "policy-1",
            isManaged = false,
            currentState = "hot",
            currentAction = "rollover",
            transitionEvaluation = null,
            nextState = null,
            error = null,
        )
        assertRoundTrip(result)
    }

    fun `test index simulate result toXContent renders error when present`() {
        val result = IndexSimulateResult(
            indexName = "missing-index",
            indexUUID = null,
            policyId = "my-policy",
            isManaged = false,
            currentState = null,
            currentAction = null,
            transitionEvaluation = null,
            nextState = null,
            error = "Index not found",
        )
        val builder = XContentFactory.jsonBuilder()
        result.toXContent(builder, ToXContent.EMPTY_PARAMS)
        val json = builder.toString()
        assertTrue(json.contains("\"${IndexSimulateResult.ERROR_FIELD}\":\"Index not found\""))
        // success fields should be absent when there is an error
        assertFalse(json.contains(IndexSimulateResult.CURRENT_STATE_FIELD))
        assertFalse(json.contains(IndexSimulateResult.CURRENT_ACTION_FIELD))
    }

    fun `test index simulate result toXContent renders success fields when no error`() {
        val transitionResult = TransitionSimulateResult("warm", false, "min_index_age", "3d", "7d")
        val result = IndexSimulateResult(
            indexName = "my-index",
            indexUUID = "uuid",
            policyId = "my-policy",
            isManaged = true,
            currentState = "hot",
            currentAction = "transition",
            transitionEvaluation = listOf(transitionResult),
            nextState = null,
            error = null,
        )
        val builder = XContentFactory.jsonBuilder()
        result.toXContent(builder, ToXContent.EMPTY_PARAMS)
        val json = builder.toString()
        assertTrue(json.contains("\"${IndexSimulateResult.CURRENT_STATE_FIELD}\":\"hot\""))
        assertTrue(json.contains("\"${IndexSimulateResult.CURRENT_ACTION_FIELD}\":\"transition\""))
        assertTrue(json.contains(IndexSimulateResult.TRANSITION_EVALUATION_FIELD))
        assertFalse(json.contains(IndexSimulateResult.ERROR_FIELD))
    }

    // ------------------------------------------------------------------ //
    // SimulatePolicyResponse                                               //
    // ------------------------------------------------------------------ //

    fun `test simulate policy response roundtrip`() {
        val transitionResult = TransitionSimulateResult("warm", true, "min_index_age", "8d", "7d")
        val indexResult = IndexSimulateResult(
            indexName = "my-index",
            indexUUID = "abc-uuid",
            policyId = "my-policy",
            isManaged = true,
            currentState = "hot",
            currentAction = "transition",
            transitionEvaluation = listOf(transitionResult),
            nextState = "warm",
            error = null,
        )
        val response = SimulatePolicyResponse(listOf(indexResult))
        val out = BytesStreamOutput()
        response.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val restored = SimulatePolicyResponse(sin)

        assertEquals(1, restored.simulateResults.size)
        val restoredResult = restored.simulateResults.first()
        assertEquals(indexResult.indexName, restoredResult.indexName)
        assertEquals(indexResult.indexUUID, restoredResult.indexUUID)
        assertEquals(indexResult.currentState, restoredResult.currentState)
        assertEquals(indexResult.nextState, restoredResult.nextState)
        assertEquals(1, restoredResult.transitionEvaluation!!.size)
        assertEquals(transitionResult.stateName, restoredResult.transitionEvaluation!!.first().stateName)
        assertEquals(transitionResult.conditionMet, restoredResult.transitionEvaluation!!.first().conditionMet)
    }

    fun `test simulate policy response roundtrip with empty results`() {
        val response = SimulatePolicyResponse(emptyList())
        val out = BytesStreamOutput()
        response.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val restored = SimulatePolicyResponse(sin)
        assertTrue(restored.simulateResults.isEmpty())
    }

    fun `test simulate policy response toXContent`() {
        val response = SimulatePolicyResponse(emptyList())
        val builder = XContentFactory.jsonBuilder()
        response.toXContent(builder, ToXContent.EMPTY_PARAMS)
        val json = builder.toString()
        assertTrue(json.contains(SimulatePolicyResponse.SIMULATE_RESULTS_FIELD))
    }

    // ------------------------------------------------------------------ //
    // Helpers                                                              //
    // ------------------------------------------------------------------ //

    private fun assertRoundTrip(result: TransitionSimulateResult) {
        val out = BytesStreamOutput()
        result.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val restored = TransitionSimulateResult(sin)
        assertEquals(result.stateName, restored.stateName)
        assertEquals(result.conditionMet, restored.conditionMet)
        assertEquals(result.conditionType, restored.conditionType)
        assertEquals(result.currentValue, restored.currentValue)
        assertEquals(result.requiredValue, restored.requiredValue)
    }

    private fun assertRoundTrip(result: IndexSimulateResult) {
        val out = BytesStreamOutput()
        result.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val restored = IndexSimulateResult(sin)
        assertEquals(result.indexName, restored.indexName)
        assertEquals(result.indexUUID, restored.indexUUID)
        assertEquals(result.policyId, restored.policyId)
        assertEquals(result.isManaged, restored.isManaged)
        assertEquals(result.currentState, restored.currentState)
        assertEquals(result.currentAction, restored.currentAction)
        assertEquals(result.nextState, restored.nextState)
        assertEquals(result.error, restored.error)
        assertEquals(result.transitionEvaluation?.size, restored.transitionEvaluation?.size)
    }
}
