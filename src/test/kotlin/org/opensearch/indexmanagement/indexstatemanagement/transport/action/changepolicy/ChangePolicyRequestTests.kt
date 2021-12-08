/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.model.StateFilter
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.test.OpenSearchTestCase

class ChangePolicyRequestTests : OpenSearchTestCase() {

    fun `test change policy request`() {
        val indices = listOf("index1", "index2")
        val stateFilter = StateFilter("state1")
        val changePolicy = ChangePolicy("policyID", "state1", listOf(stateFilter), true)
        val req = ChangePolicyRequest(indices, changePolicy, DEFAULT_INDEX_TYPE)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = ChangePolicyRequest(sin)
        assertEquals(indices, newReq.indices)
        assertEquals(changePolicy, newReq.changePolicy)
    }

    fun `test change policy request with non default index type and multiple indices fails`() {
        val indices = listOf("index1", "index2")
        val stateFilter = StateFilter("state1")
        val changePolicy = ChangePolicy("policyID", "state1", listOf(stateFilter), true)
        val req = ChangePolicyRequest(indices, changePolicy, "non-existent-index-type")
        val actualException: String? = req.validate()?.validationErrors()?.firstOrNull()
        val expectedException: String = ChangePolicyRequest.MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR
        assertEquals("Add policy request should have failed validation with specific exception", actualException, expectedException)
    }
}
