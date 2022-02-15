/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.transform.action.explain.ExplainTransformResponse
import org.opensearch.indexmanagement.transform.action.get.GetTransformResponse
import org.opensearch.indexmanagement.transform.action.get.GetTransformsResponse
import org.opensearch.indexmanagement.transform.action.index.IndexTransformResponse
import org.opensearch.indexmanagement.transform.action.preview.PreviewTransformResponse
import org.opensearch.indexmanagement.transform.buildStreamInputForTransforms
import org.opensearch.indexmanagement.transform.randomExplainTransform
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.rest.RestStatus
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.OpenSearchTestCase.randomList

class ResponseTests : OpenSearchTestCase() {

    fun `test explain transform response`() {
        val idsToExplain = randomList(10) { randomAlphaOfLength(10) to randomExplainTransform() }.toMap()
        val failedToExplain = randomList(10) { randomAlphaOfLength(10) to randomAlphaOfLength(10) }.toMap()
        val res = ExplainTransformResponse(idsToExplain, failedToExplain)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val streamedRes = ExplainTransformResponse(buildStreamInputForTransforms(out))

        assertEquals(idsToExplain, streamedRes.idsToExplain)
    }

    fun `test index transform response`() {
        val transform = randomTransform()
        val res = IndexTransformResponse("someid", 1L, 2L, 3L, RestStatus.OK, transform)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val streamedRes = IndexTransformResponse(buildStreamInputForTransforms(out))
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(transform, streamedRes.transform)
    }

    fun `test preview transform response`() {
        val documents = listOf(
            mapOf("a" to mapOf<String, Any>("90.0" to 100), "b" to "id1", "c" to 100),
            mapOf("a" to mapOf<String, Any>("90.0" to 50), "b" to "id2", "c" to 20)
        )
        val res = PreviewTransformResponse(documents, RestStatus.OK)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedRes = PreviewTransformResponse(sin)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(documents, streamedRes.documents)
    }

    fun `test get transform response null`() {
        val res = GetTransformResponse("someid", 1L, 2L, 3L, RestStatus.OK, null)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val streamedRes = GetTransformResponse(buildStreamInputForTransforms(out))
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(null, streamedRes.transform)
    }

    fun `test get transform response`() {
        val transform = randomTransform()
        val res = GetTransformResponse("someid", 1L, 2L, 3L, RestStatus.OK, transform)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val streamedRes = GetTransformResponse(buildStreamInputForTransforms(out))
        assertEquals("someid", streamedRes.id)
        assertEquals(1L, streamedRes.version)
        assertEquals(2L, streamedRes.seqNo)
        assertEquals(3L, streamedRes.primaryTerm)
        assertEquals(RestStatus.OK, streamedRes.status)
        assertEquals(transform, streamedRes.transform)
    }

    fun `test get transforms response`() {
        val transforms = randomList(1, 15) { randomTransform() }

        val res = GetTransformsResponse(transforms, transforms.size, RestStatus.OK)
        val out = BytesStreamOutput().apply { res.writeTo(this) }
        val streamedRes = GetTransformsResponse(buildStreamInputForTransforms(out))

        assertEquals(transforms.size, streamedRes.totalTransforms)
        assertEquals(transforms.size, streamedRes.transforms.size)
        assertEquals(RestStatus.OK, streamedRes.status)
        for (i in 0 until transforms.size) {
            assertEquals(transforms[i], streamedRes.transforms[i])
        }
    }
}
