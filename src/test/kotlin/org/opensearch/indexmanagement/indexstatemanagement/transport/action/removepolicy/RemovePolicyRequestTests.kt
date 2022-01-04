/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.test.OpenSearchTestCase

class RemovePolicyRequestTests : OpenSearchTestCase() {

    fun `test remove policy request`() {
        val indices = listOf("index1", "index2")
        val req = RemovePolicyRequest(indices, DEFAULT_INDEX_TYPE)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = RemovePolicyRequest(sin)
        assertEquals(indices, newReq.indices)
    }

    fun `test remove policy request with non default index type and multiple indices fails`() {
        val indices = listOf("index1", "index2")
        val req = RemovePolicyRequest(indices, "non-existent-index-type")
        val actualException: String? = req.validate()?.validationErrors()?.firstOrNull()
        val expectedException: String = RemovePolicyRequest.MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR
        assertEquals("Remove policy request should have failed validation with specific exception", actualException, expectedException)
    }
}
