/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.opensearch.test.OpenSearchTestCase

class ISMStatusResponseTests : OpenSearchTestCase() {

    fun `test ISM status response`() {
        val updated = 1
        val failedIndex = FailedIndex("index", "uuid", "reason")
        val failedIndices = mutableListOf(failedIndex)

        val res = ISMStatusResponse(updated, failedIndices)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = ISMStatusResponse(sin)
        assertEquals(updated, newRes.updated)
        assertEquals(failedIndices, newRes.failedIndices)
    }
}
