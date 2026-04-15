/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.test.OpenSearchTestCase

class SimulatePolicyRequestTests : OpenSearchTestCase() {
    private val indexName = "test-index"

    fun `test request serialization roundtrip with policy id`() {
        val request = SimulatePolicyRequest(
            indices = listOf(indexName, "index-2"),
            policyId = "my-policy",
            policy = null,
        )
        val out = BytesStreamOutput()
        request.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val restored = SimulatePolicyRequest(sin)

        assertEquals(request.indices, restored.indices)
        assertEquals(request.policyId, restored.policyId)
        assertNull(restored.policy)
    }

    fun `test request serialization roundtrip with inline policy`() {
        val policy = randomPolicy(states = listOf(randomState()))
        val request = SimulatePolicyRequest(
            indices = listOf(indexName),
            policyId = null,
            policy = policy,
        )
        val out = BytesStreamOutput()
        request.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val restored = SimulatePolicyRequest(sin)

        assertEquals(request.indices, restored.indices)
        assertNull(restored.policyId)
        assertNotNull(restored.policy)
        assertEquals(policy.id, restored.policy!!.id)
    }

    fun `test validate fails when both policy id and policy are null`() {
        val request = SimulatePolicyRequest(
            indices = listOf(indexName),
            policyId = null,
            policy = null,
        )
        val exception = request.validate()
        assertNotNull("Expected validation error", exception)
        assertTrue(exception!!.validationErrors().any { it.contains("either policy_id or policy") })
    }

    fun `test validate fails when both policy id and policy are provided`() {
        val request = SimulatePolicyRequest(
            indices = listOf(indexName),
            policyId = "my-policy",
            policy = randomPolicy(states = listOf(randomState())),
        )
        val exception = request.validate()
        assertNotNull("Expected validation error", exception)
        assertTrue(exception!!.validationErrors().any { it.contains("only one of policy_id or policy") })
    }

    fun `test validate fails when indices are empty`() {
        val request = SimulatePolicyRequest(
            indices = emptyList(),
            policyId = "my-policy",
            policy = null,
        )
        val exception = request.validate()
        assertNotNull("Expected validation error", exception)
        assertTrue(exception!!.validationErrors().any { it.contains("indices cannot be empty") })
    }

    fun `test validate passes for valid request with policy id`() {
        val request = SimulatePolicyRequest(
            indices = listOf(indexName),
            policyId = "my-policy",
            policy = null,
        )
        assertNull("Expected no validation error", request.validate())
    }

    fun `test validate passes for valid request with inline policy`() {
        val request = SimulatePolicyRequest(
            indices = listOf(indexName),
            policyId = null,
            policy = randomPolicy(states = listOf(randomState())),
        )
        assertNull("Expected no validation error", request.validate())
    }
}
