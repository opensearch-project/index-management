/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class AddPolicyRequestTests : OpenSearchTestCase() {

    fun `test add policy request`() {
        val indices = listOf("index1", "index2")
        val policyID = "policyID"
        val req = AddPolicyRequest(indices, policyID)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = AddPolicyRequest(sin)
        assertEquals(indices, newReq.indices)
        assertEquals(policyID, newReq.policyID)
    }
}
