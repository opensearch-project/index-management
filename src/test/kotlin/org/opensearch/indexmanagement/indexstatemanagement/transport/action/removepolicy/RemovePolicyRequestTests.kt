/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class RemovePolicyRequestTests : OpenSearchTestCase() {

    fun `test remove policy request`() {
        val indices = listOf("index1", "index2")
        val req = RemovePolicyRequest(indices)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = RemovePolicyRequest(sin)
        assertEquals(indices, newReq.indices)
    }
}
