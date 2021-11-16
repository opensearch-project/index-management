/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.test.OpenSearchTestCase

class GetPoliciesResponseTests : OpenSearchTestCase() {

    fun `test get policies response`() {
        val policy = randomPolicy()
        val res = GetPoliciesResponse(listOf(policy), 1)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = GetPoliciesResponse(sin)
        assertEquals(1, newRes.totalPolicies)
        assertEquals(1, newRes.policies.size)
        assertEquals(policy, newRes.policies[0])
    }
}
