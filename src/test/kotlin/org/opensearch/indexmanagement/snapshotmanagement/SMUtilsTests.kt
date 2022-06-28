/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.randomCronSchedule
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant.now
import kotlin.test.assertFailsWith

class SMUtilsTests : OpenSearchTestCase() {
    fun `test sm policy name and id conversion`() {
        val policyName = "daily-snapshot-sm-sm"
        assertEquals(policyName, smDocIdToPolicyName(smPolicyNameToDocId(policyName)))
        assertEquals(policyName, smMetadataDocIdToPolicyName(smPolicyNameToMetadataDocId(policyName)))

        val policyNameUnderscores = "daily-snapshot_sm_policy"
        assertEquals(policyNameUnderscores, smDocIdToPolicyName(smPolicyNameToDocId(policyNameUnderscores)))
        assertEquals(policyNameUnderscores, smMetadataDocIdToPolicyName(smPolicyNameToMetadataDocId(policyNameUnderscores)))
    }

    fun `test snapshot name date_format`() {
        assertFailsWith<IllegalArgumentException> {
            val smPolicy = randomSMPolicy(dateFormat = " ")
            val smPolicyString = smPolicy.toJsonString()
            smPolicyString.parser().parseWithType(smPolicy.id, smPolicy.seqNo, smPolicy.primaryTerm, SMPolicy.Companion::parse)
        }
    }

    fun `test valid policy name`() {
        val policyName = "bowen #"
        assertFailsWith<IllegalArgumentException> {
            validateSMPolicyName(policyName)
        }
    }

    fun `test random cron expression`() {
        val randomCronSchedule = randomCronSchedule()
        // val randomCronSchedule = CronSchedule("14 13 31 2 *", randomZone())
        val nextTime = randomCronSchedule.getNextExecutionTime(now())
        assertNotNull("next time should not be null", nextTime)
    }
}
