/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action

import org.opensearch.action.DocWriteRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.transform.action.delete.DeleteTransformsRequest
import org.opensearch.indexmanagement.transform.action.explain.ExplainTransformRequest
import org.opensearch.indexmanagement.transform.action.get.GetTransformRequest
import org.opensearch.indexmanagement.transform.action.get.GetTransformsRequest
import org.opensearch.indexmanagement.transform.action.index.IndexTransformRequest
import org.opensearch.indexmanagement.transform.action.preview.PreviewTransformRequest
import org.opensearch.indexmanagement.transform.action.start.StartTransformRequest
import org.opensearch.indexmanagement.transform.action.stop.StopTransformRequest
import org.opensearch.indexmanagement.transform.buildStreamInputForTransforms
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.test.OpenSearchTestCase

class RequestTests : OpenSearchTestCase() {

    fun `test delete single transform request`() {
        val id = "some_id"
        val req = DeleteTransformsRequest(listOf(id), false)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = DeleteTransformsRequest(buildStreamInputForTransforms(out))
        assertEquals(listOf(id), streamedReq.ids)
    }

    fun `test delete multiple transform request`() {
        val ids = mutableListOf("some_id", "some_other_id")
        val req = DeleteTransformsRequest(ids, true)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = DeleteTransformsRequest(buildStreamInputForTransforms(out))
        assertEquals(ids, streamedReq.ids)
    }

    fun `test explain transform request`() {
        val ids = listOf("oneid", "twoid", "threeid")
        val req = ExplainTransformRequest(ids)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = ExplainTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(ids, streamedReq.transformIDs)
    }

    fun `test index transform create request`() {
        val transform = randomTransform().copy(seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val req = IndexTransformRequest(
            transform = transform,
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        ).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = IndexTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(transform, streamedReq.transform)
        assertEquals(transform.seqNo, streamedReq.ifSeqNo())
        assertEquals(transform.primaryTerm, streamedReq.ifPrimaryTerm())
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, streamedReq.refreshPolicy)
        assertEquals(DocWriteRequest.OpType.CREATE, streamedReq.opType())
    }

    fun `test index transform update request`() {
        val transform = randomTransform().copy(seqNo = 1L, primaryTerm = 2L)
        val req = IndexTransformRequest(
            transform = transform,
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        ).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = IndexTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(transform, streamedReq.transform)
        assertEquals(transform.seqNo, streamedReq.ifSeqNo())
        assertEquals(transform.primaryTerm, streamedReq.ifPrimaryTerm())
        assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, streamedReq.refreshPolicy)
        assertEquals(DocWriteRequest.OpType.INDEX, streamedReq.opType())
    }

    fun `test preview transform request`() {
        val transform = randomTransform()
        val req = PreviewTransformRequest(transform = transform)
        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = PreviewTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(transform, streamedReq.transform)
    }

    fun `test get transform request`() {
        val id = "some_id"
        val srcContext = null
        val preference = "_local"
        val req = GetTransformRequest(id, srcContext, preference)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = GetTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(id, streamedReq.id)
        assertEquals(srcContext, streamedReq.srcContext)
        assertEquals(preference, streamedReq.preference)
    }

    fun `test head get transform request`() {
        val id = "some_id"
        val srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        val req = GetTransformRequest(id, srcContext)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = GetTransformRequest(buildStreamInputForTransforms(out))
        assertEquals(id, streamedReq.id)
        assertEquals(srcContext, streamedReq.srcContext)
    }

    fun `test get transforms request default`() {
        val req = GetTransformsRequest()

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = GetTransformsRequest(buildStreamInputForTransforms(out))
        assertEquals("", streamedReq.searchString)
        assertEquals(0, streamedReq.from)
        assertEquals(20, streamedReq.size)
        assertEquals("${Transform.TRANSFORM_TYPE}.${Transform.TRANSFORM_ID_FIELD}.keyword", streamedReq.sortField)
        assertEquals("asc", streamedReq.sortDirection)
    }

    fun `test get transforms request`() {
        val req = GetTransformsRequest("searching", 10, 50, "sorted", "desc")

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = GetTransformsRequest(buildStreamInputForTransforms(out))
        assertEquals("searching", streamedReq.searchString)
        assertEquals(10, streamedReq.from)
        assertEquals(50, streamedReq.size)
        assertEquals("sorted", streamedReq.sortField)
        assertEquals("desc", streamedReq.sortDirection)
    }

    fun `test empty ids delete transforms request`() {
        val req = DeleteTransformsRequest(listOf(), false)
        val validated = req.validate()
        assertNotNull("Expected validate to produce Exception", validated)
        assertEquals("org.opensearch.action.ActionRequestValidationException: Validation Failed: 1: List of ids to delete is empty;", validated.toString())
    }

    fun `test start transform request`() {
        val id = "some_id"
        val req = StartTransformRequest(id).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = StartTransformRequest(buildStreamInputForTransforms(out))

        assertEquals(id, streamedReq.id())
    }

    fun `test stop transform request`() {
        val id = "some_id"
        val req = StopTransformRequest(id).index(INDEX_MANAGEMENT_INDEX)

        val out = BytesStreamOutput().apply { req.writeTo(this) }
        val streamedReq = StopTransformRequest(buildStreamInputForTransforms(out))

        assertEquals(id, streamedReq.id())
    }
}
