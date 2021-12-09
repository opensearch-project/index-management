/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.indexstatemanagement.action.IndexPriorityAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

class IndexPolicyResponseTests : OpenSearchTestCase() {

    // TODO: fixme - enable the test
    private fun `test index policy response index priority action`() {
        val id = "id"
        val version: Long = 1
        val primaryTerm: Long = 123
        val seqNo: Long = 456
        val policyID = "policyID"
        val actionConfig = IndexPriorityAction(50, 0)
        val states = listOf(State(name = "SetPriorityState", actions = listOf(actionConfig), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        val status = RestStatus.CREATED

        val res = IndexPolicyResponse(id, version, primaryTerm, seqNo, policy, status)

        val out = BytesStreamOutput()
        res.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRes = IndexPolicyResponse(sin)
        assertEquals(id, newRes.id)
        assertEquals(version, newRes.version)
        assertEquals(primaryTerm, newRes.primaryTerm)
        assertEquals(seqNo, newRes.seqNo)
        assertEquals(policy, newRes.policy)
        assertEquals(status, newRes.status)
    }
}
