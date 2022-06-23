/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.action

import org.opensearch.action.DocWriteRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.common.model.rest.SearchParams
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete.DeleteSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPoliciesRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.start.StartSMRequest
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.stop.StopSMRequest
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.test.OpenSearchTestCase

class RequestTests : OpenSearchTestCase() {

    fun `test delete sm policy request`() {
        val id = "some_id"
        val req = DeleteSMPolicyRequest(id).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = DeleteSMPolicyRequest(sin)
        assertEquals(id, streamedReq.id())
    }

    fun `test get sm policy request`() {
        val id = "some_id"
        val req = GetSMPolicyRequest(id)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = GetSMPolicyRequest(sin)
        assertEquals(id, streamedReq.policyID)
    }

    fun `test get all sm policy request`() {
        val searchParams = SearchParams(0, 20, "sort-field", "asc", "*")
        val req = GetSMPoliciesRequest(searchParams)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        GetSMPoliciesRequest(sin)
    }

    fun `test index sm policy put request`() {
        val smPolicy = randomSMPolicy().copy(seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val req = IndexSMPolicyRequest(policy = smPolicy, false, WriteRequest.RefreshPolicy.IMMEDIATE).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = IndexSMPolicyRequest(sin)
        assertEquals(smPolicy, streamedReq.policy)
        assertEquals(smPolicy.seqNo, streamedReq.ifSeqNo())
        assertEquals(smPolicy.primaryTerm, streamedReq.ifPrimaryTerm())
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, streamedReq.refreshPolicy)
        assertEquals(DocWriteRequest.OpType.INDEX, streamedReq.opType())
    }

    fun `test index sm policy post request`() {
        val smPolicy = randomSMPolicy().copy(seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val req = IndexSMPolicyRequest(policy = smPolicy, true, WriteRequest.RefreshPolicy.IMMEDIATE).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = IndexSMPolicyRequest(sin)
        assertEquals(smPolicy, streamedReq.policy)
        assertEquals(smPolicy.seqNo, streamedReq.ifSeqNo())
        assertEquals(smPolicy.primaryTerm, streamedReq.ifPrimaryTerm())
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, streamedReq.refreshPolicy)
        assertEquals(DocWriteRequest.OpType.CREATE, streamedReq.opType())
    }

    fun `test start sm policy request`() {
        val id = "some_id"
        val req = StartSMRequest(id).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = StartSMRequest(sin)
        assertEquals(id, streamedReq.id())
    }

    fun `test stop sm policy request`() {
        val id = "some_id"
        val req = StopSMRequest(id).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = StopSMRequest(sin)
        assertEquals(id, streamedReq.id())
    }

    fun `test explain sm policy request`() {
        val policyNames = randomList(1, 10) { randomAlphaOfLength(10) }
        val req = ExplainSMPolicyRequest(policyNames.toTypedArray())

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedReq = ExplainSMPolicyRequest(sin)
        policyNames.forEach { name ->
            assertTrue(streamedReq.policyNames.contains(name))
        }
    }
}
