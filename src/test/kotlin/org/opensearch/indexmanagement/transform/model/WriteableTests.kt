/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.indexmanagement.transform.buildStreamInputForTransforms
import org.opensearch.indexmanagement.transform.randomISMTransform
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.indexmanagement.transform.randomTransformMetadata
import org.opensearch.test.OpenSearchTestCase

class WriteableTests : OpenSearchTestCase() {
    fun `test transform metadata as stream`() {
        val transformMetadata = randomTransformMetadata()
        val out = BytesStreamOutput().also { transformMetadata.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedTransformMetadata = TransformMetadata(sin)
        assertEquals("Round tripping TransformMetadata stream doesn't work", transformMetadata, streamedTransformMetadata)
    }

    fun `test transform as stream`() {
        val transform = randomTransform()
        val out = BytesStreamOutput().also { transform.writeTo(it) }
        val streamedTransform = Transform(buildStreamInputForTransforms(out))
        assertEquals("Round tripping Transform stream doesn't work", transform, streamedTransform)
    }

    fun `test transform roles field deprecation`() {
        val transform = randomTransform().copy(roles = listOf("I am deprecated"))
        val out = BytesStreamOutput().also { transform.writeTo(it) }
        val streamedTransform = Transform(buildStreamInputForTransforms(out))
        @Suppress("DEPRECATION")
        assertTrue("roles field in transform model is deprecated and should be parsed to empty list.", streamedTransform.roles.isEmpty())
    }

    fun `test ism transform as stream`() {
        val ismTransform = randomISMTransform()
        val out = BytesStreamOutput().also { ismTransform.writeTo(it) }
        val streamedISMTransform = ISMTransform(buildStreamInputForTransforms(out))
        assertEquals("Round tripping ISMTransform stream doesn't work", ismTransform, streamedISMTransform)
    }
}
