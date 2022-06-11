/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.common.model.rest.SearchParams
import org.opensearch.test.OpenSearchTestCase

class GetPoliciesRequestTests : OpenSearchTestCase() {

    fun `test get policies request`() {
        val table = SearchParams(20, 0, "policy.policy_id.keyword", "desc", "*")
        val req = GetPoliciesRequest(table)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = GetPoliciesRequest(sin)
        assertEquals(table, newReq.searchParams)
    }
}
