/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.test.OpenSearchTestCase

class AddPolicyRequestTests : OpenSearchTestCase() {

    fun `test add policy request`() {
        val indices = listOf("index1", "index2")
        val policyID = "policyID"
        val req = AddPolicyRequest(indices, policyID, DEFAULT_INDEX_TYPE)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = AddPolicyRequest(sin)
        assertEquals(indices, newReq.indices)
        assertEquals(policyID, newReq.policyID)
    }

    fun `test add policy request with non default index type and multiple indices fails`() {
        val indices = listOf("index1", "index2")
        val policyID = "policyID"
        val req = AddPolicyRequest(indices, policyID, "non-existent-index-type")
        val actualException: String? = req.validate()?.validationErrors()?.firstOrNull()
        val expectedException: String = AddPolicyRequest.MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR
        assertEquals("Add policy request should have failed validation with specific exception", actualException, expectedException)
    }
}
