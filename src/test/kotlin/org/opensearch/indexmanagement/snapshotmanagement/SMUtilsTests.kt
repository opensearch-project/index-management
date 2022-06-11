/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

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
}
