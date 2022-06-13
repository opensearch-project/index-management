/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.model.ExplainSMPolicy
import org.opensearch.test.OpenSearchTestCase

class SMUtilsTests : OpenSearchTestCase() {
    fun `test sm policy name and id conversion`() {
        val policyName = "daily-snapshot-sm-sm"
        assertEquals(policyName, smDocIdToPolicyName(smPolicyNameToDocId(policyName)))
        assertEquals(policyName, smMetadataIdToPolicyName(smPolicyNameToMetadataId(policyName)))

        val policyNameUnderscores = "daily-snapshot_sm_policy"
        assertEquals(policyNameUnderscores, smDocIdToPolicyName(smPolicyNameToDocId(policyNameUnderscores)))
        assertEquals(policyNameUnderscores, smMetadataIdToPolicyName(smPolicyNameToMetadataId(policyNameUnderscores)))
    }

    fun `test snapshot name date_format`() {
        val timeStr = generateFormatTime("")
        assertEquals("Generate time string", "invalid_date_format", timeStr)
    }

    fun `test parse metadata in explain response`() {
        val metadata = randomSMMetadata()
        val policiesToExplain: Map<String, ExplainSMPolicy?> = mapOf("policyName" to ExplainSMPolicy(metadata, true))
        val response = ExplainSMPolicyResponse(policiesToExplain)
        println("SIN $policiesToExplain")
        val out = BytesStreamOutput().also { response.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        // parseSMMetadata(sin)
    }
}
