/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.unit.TimeValue
import org.opensearch.test.OpenSearchTestCase

class RetryFailedManagedIndexRequestTests : OpenSearchTestCase() {

    fun `test retry managed index request`() {
        val indices = listOf("index1", "index2")
        val startState = "state1"
        val masterTimeout = TimeValue.timeValueSeconds(30)
        val req = RetryFailedManagedIndexRequest(indices, startState, masterTimeout)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = RetryFailedManagedIndexRequest(sin)
        assertEquals(indices, newReq.indices)
        assertEquals(startState, newReq.startState)
    }
}
