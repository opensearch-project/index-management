/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.refreshanalyzer

import org.opensearch.Version
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class RefreshSearchAnalyzerRequestTests : OpenSearchTestCase() {

    fun `test serialization with reloadCachedResources true`() {
        val request = RefreshSearchAnalyzerRequest("test-index", reloadCachedResources = true)

        val out = BytesStreamOutput()
        out.setVersion(Version.V_3_7_0)
        request.writeTo(out)

        val inp: StreamInput = out.bytes().streamInput()
        inp.setVersion(Version.V_3_7_0)
        val deserialized = RefreshSearchAnalyzerRequest(inp)

        assertTrue(deserialized.reloadCachedResources)
    }

    fun `test serialization with reloadCachedResources false`() {
        val request = RefreshSearchAnalyzerRequest("test-index", reloadCachedResources = false)

        val out = BytesStreamOutput()
        out.setVersion(Version.V_3_7_0)
        request.writeTo(out)

        val inp: StreamInput = out.bytes().streamInput()
        inp.setVersion(Version.V_3_7_0)
        val deserialized = RefreshSearchAnalyzerRequest(inp)

        assertFalse(deserialized.reloadCachedResources)
    }

    fun `test BWC deserialization from older version defaults to false`() {
        val request = RefreshSearchAnalyzerRequest("test-index", reloadCachedResources = true)

        // Write with current version
        val out = BytesStreamOutput()
        out.setVersion(Version.V_3_6_0)
        request.writeTo(out)

        // Read with older version — should default to false
        val inp: StreamInput = out.bytes().streamInput()
        inp.setVersion(Version.V_3_6_0)
        val deserialized = RefreshSearchAnalyzerRequest(inp)

        assertFalse(deserialized.reloadCachedResources)
    }
}
