/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.executemanagedindex

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class ExecuteManagedIndexRequestTests : OpenSearchTestCase() {
    fun `test execute managed index request`() {
        val req = ExecuteManagedIndexRequest("index1")

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = ExecuteManagedIndexRequest(sin)
        assertEquals("index1", newReq.index)
    }

    fun `test valid request passes validation`() {
        assertNull(ExecuteManagedIndexRequest("index1").validate())
    }

    fun `test blank index fails validation`() {
        val error = ExecuteManagedIndexRequest(" ").validate()?.validationErrors()?.firstOrNull()
        assertEquals("Missing index", error)
    }

    fun `test wildcard and list fail validation`() {
        listOf("index*", "index1,index2").forEach {
            assertNotNull("Expected [$it] to fail validation", ExecuteManagedIndexRequest(it).validate())
        }
    }
}
