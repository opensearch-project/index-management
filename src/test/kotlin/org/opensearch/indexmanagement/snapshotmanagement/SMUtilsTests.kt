/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.indexmanagement.randomCronSchedule
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.model.ExplainSMPolicy
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant.now
import kotlin.test.assertFailsWith

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

    fun `test valid policy name`() {
        val policyName = "bowen #"
        assertFailsWith<IllegalArgumentException> {
            validateSMPolicyName(policyName)
        }
    }

    fun `test parse metadata in explain response`() {
        val metadata = randomSMMetadata()
        val policiesToExplain: Map<String, ExplainSMPolicy?> = mapOf("policyName" to ExplainSMPolicy(metadata, true))
        val response = ExplainSMPolicyResponse(policiesToExplain)
        val out = BytesStreamOutput().also { response.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        // parseSMMetadata(sin)
    }

    fun `test random cron expression`() {
        val randomCronSchedule = randomCronSchedule()
        // val randomCronSchedule = CronSchedule("14 13 31 2 *", randomZone())
        val nextTime = randomCronSchedule.getNextExecutionTime(now())
        println(randomCronSchedule.cronExpression)
        assertNotNull("next time should not be null", nextTime)
    }
}
