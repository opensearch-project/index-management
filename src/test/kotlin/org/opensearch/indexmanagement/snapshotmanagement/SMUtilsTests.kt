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
    }

    fun `test snapshot name date_format`() {
        val timeStr = generateFormatTime(randomAlphaOfLength(8))
        assertEquals("Generate time string", "invalid_date_format", timeStr)
    }
}
