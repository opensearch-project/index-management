/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.snapshotmanagement.randomNotificationConfig
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.test.OpenSearchTestCase

class WriteableTests : OpenSearchTestCase() {

    fun `test sm policy as stream`() {
        val smPolicy = randomSMPolicy(notificationConfig = randomNotificationConfig())
        val out = BytesStreamOutput().also { smPolicy.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedSMPolicy = SMPolicy(sin)
        assertEquals("Round tripping sm policy stream doesn't work", smPolicy, streamedSMPolicy)
    }

    fun `test sm metadata as stream`() {
        val smMetadata = randomSMMetadata()
        val out = BytesStreamOutput().also { smMetadata.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedSMMetadata = SMMetadata(sin)
        assertEquals("Round tripping sm metadata stream doesn't work", smMetadata, streamedSMMetadata)
    }
}
