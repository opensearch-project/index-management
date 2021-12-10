/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy

import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class DeletePolicyRequestTests : OpenSearchTestCase() {

    fun `test delete policy request`() {
        val policyID = "policyID"
        val refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        val req = DeletePolicyRequest(policyID, refreshPolicy)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = DeletePolicyRequest(sin)
        assertEquals(policyID, newReq.policyID)
        assertEquals(refreshPolicy, newReq.refreshPolicy)
    }
}
