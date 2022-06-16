/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.action

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPoliciesResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.model.ExplainSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.indexmanagement.snapshotmanagement.toMap
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase

class ResponseTests : OpenSearchTestCase() {

    fun `test index sm policy response`() {
        val smPolicy = randomSMPolicy()
        val res = IndexSMPolicyResponse("someid", 1L, 2L, 3L, smPolicy, RestStatus.OK)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = IndexSMPolicyResponse(sin)
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(smPolicy, streamedRes.policy)
    }

    fun `test index sm policy toXContent`() {
        val smPolicy = randomSMPolicy()
        val res = IndexSMPolicyResponse("someid", 1L, 2L, 3L, smPolicy, RestStatus.OK)
        val resMap = res.toMap()
        assertEquals(resMap["sm_policy"], smPolicy.toMap(XCONTENT_WITHOUT_TYPE_AND_USER))
    }

    fun `test get sm policy response`() {
        val smPolicy = randomSMPolicy()
        val res = GetSMPolicyResponse("someid", 1L, 2L, 3L, smPolicy)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = GetSMPolicyResponse(sin)
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(smPolicy, streamedRes.policy)
    }

    fun `test get all sm policy response`() {
        val smPolicies = randomList(1, 15) { randomSMPolicy() }
        val res = GetSMPoliciesResponse(smPolicies, smPolicies.size.toLong())
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = GetSMPoliciesResponse(sin)
        assertEquals(res.totalPolicies, streamedRes.totalPolicies)
        assertEquals(smPolicies.size.toLong(), res.totalPolicies)
        assertEquals(res.policies, streamedRes.policies)
        assertEquals(smPolicies, res.policies)
    }

    fun `test explain sm policy response`() {
        val smMetadata = randomSMMetadata()
        val explainSMPolicy = ExplainSMPolicy(smMetadata, randomBoolean())
        val explainMap: Map<String, ExplainSMPolicy> = listOf(smDocIdToPolicyName(smMetadata.id) to explainSMPolicy).toMap()
        val res = ExplainSMPolicyResponse(explainMap)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = ExplainSMPolicyResponse(sin)
        assertEquals(res.policiesToExplain, streamedRes.policiesToExplain)
    }

    fun `test get sm policy toXContent`() {
        val smPolicy = randomSMPolicy()
        val res = GetSMPolicyResponse("someid", 1L, 2L, 3L, smPolicy)
        val resMap = res.toMap()
        assertEquals(resMap["sm_policy"], smPolicy.toMap(XCONTENT_WITHOUT_TYPE_AND_USER))
    }
}
