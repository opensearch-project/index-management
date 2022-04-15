/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy

import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.IndexPriorityAction
import org.opensearch.indexmanagement.indexstatemanagement.extension.SampleCustomActionParser
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

class IndexPolicyRequestTests : OpenSearchTestCase() {

    fun `test index policy request index priority action`() {
        val policyID = "policyID"
        val action = IndexPriorityAction(50, 0)
        val states = listOf(State(name = "SetPriorityState", actions = listOf(action), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        val seqNo: Long = 123
        val primaryTerm: Long = 456
        val refreshPolicy = WriteRequest.RefreshPolicy.NONE
        val req = IndexPolicyRequest(policyID, policy, seqNo, primaryTerm, refreshPolicy)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexPolicyRequest(sin)
        assertEquals(policyID, newReq.policyID)
        assertEquals(policy.convertToMap(), newReq.policy.convertToMap())
        assertEquals(seqNo, newReq.seqNo)
        assertEquals(primaryTerm, newReq.primaryTerm)
    }

    fun `test index policy request allocation action`() {
        val policyID = "policyID"
        val action = AllocationAction(require = mapOf("box_type" to "hot"), exclude = emptyMap(), include = emptyMap(), index = 0)
        val states = listOf(State("Allocate", listOf(action), listOf()))

        val policy = Policy(
            id = policyID,
            description = "description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        val seqNo: Long = 123
        val primaryTerm: Long = 456
        val refreshPolicy = WriteRequest.RefreshPolicy.NONE
        val req = IndexPolicyRequest(policyID, policy, seqNo, primaryTerm, refreshPolicy)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexPolicyRequest(sin)
        assertEquals(policyID, newReq.policyID)
        assertEquals(policy.convertToMap(), newReq.policy.convertToMap())
        assertEquals(seqNo, newReq.seqNo)
        assertEquals(primaryTerm, newReq.primaryTerm)
    }

    fun `test index policy request delete action`() {
        val policyID = "policyID"
        val action = DeleteAction(index = 0)
        val states = listOf(State("Delete", listOf(action), listOf()))

        val policy = Policy(
            id = policyID,
            description = "description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        val seqNo: Long = 123
        val primaryTerm: Long = 456
        val refreshPolicy = WriteRequest.RefreshPolicy.NONE
        val req = IndexPolicyRequest(policyID, policy, seqNo, primaryTerm, refreshPolicy)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexPolicyRequest(sin)
        assertEquals(policyID, newReq.policyID)
        assertEquals(policy.convertToMap(), newReq.policy.convertToMap())
        assertEquals(seqNo, newReq.seqNo)
        assertEquals(primaryTerm, newReq.primaryTerm)
    }

    fun `test index policy request custom action`() {
        val customActionParser = SampleCustomActionParser()
        val extensionName = "testExtension"
        ISMActionsParser.instance.addParser(customActionParser, extensionName)
        val policyID = "policyID"
        val action = SampleCustomActionParser.SampleCustomAction(someInt = randomInt(), index = 0)
        val states = listOf(State("MyState", listOf(action), listOf()))

        val policy = Policy(
            id = policyID,
            description = "description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )
        val seqNo: Long = 123
        val primaryTerm: Long = 456
        val refreshPolicy = WriteRequest.RefreshPolicy.NONE
        val req = IndexPolicyRequest(policyID, policy, seqNo, primaryTerm, refreshPolicy)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = IndexPolicyRequest(sin)
        assertEquals(policyID, newReq.policyID)
        assertEquals(policy.convertToMap(), newReq.policy.convertToMap())
        assertEquals(seqNo, newReq.seqNo)
        assertEquals(primaryTerm, newReq.primaryTerm)

        ISMActionsParser.instance.parsers.removeIf { it.getActionType() == SampleCustomActionParser.SampleCustomAction.name }
    }
}
