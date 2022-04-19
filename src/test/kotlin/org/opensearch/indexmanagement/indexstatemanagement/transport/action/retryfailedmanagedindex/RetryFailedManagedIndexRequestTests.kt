/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.test.OpenSearchTestCase

class RetryFailedManagedIndexRequestTests : OpenSearchTestCase() {

    fun `test retry managed index request`() {
        val indices = listOf("index1", "index2")
        val startState = "state1"
        val clusterManagerTimeout = TimeValue.timeValueSeconds(30)
        val req = RetryFailedManagedIndexRequest(indices, startState, clusterManagerTimeout, DEFAULT_INDEX_TYPE)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = RetryFailedManagedIndexRequest(sin)
        assertEquals(indices, newReq.indices)
        assertEquals(startState, newReq.startState)
    }

    fun `test retry managed index request with non default index type and multiple indices fails`() {
        val indices = listOf("index1", "index2")
        val startState = "state1"
        val clusterManagerTimeout = TimeValue.timeValueSeconds(30)
        val req = RetryFailedManagedIndexRequest(indices, startState, clusterManagerTimeout, "non-existent-index-type")

        val actualException: String? = req.validate()?.validationErrors()?.firstOrNull()
        val expectedException: String = RetryFailedManagedIndexRequest.MULTIPLE_INDICES_CUSTOM_INDEX_TYPE_ERROR
        assertEquals("Retry failed managed index request should have failed validation with specific exception", actualException, expectedException)
    }
}
