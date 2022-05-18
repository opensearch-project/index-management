/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.opensearch.test.OpenSearchTestCase

class SMUtilsTests : OpenSearchTestCase() {
    fun `test sm policy name and id conversion`() {
        val policyName = "daily-snapshot-sm-sm"
        assertEquals(policyName, smJobIdToPolicyName(smPolicyNameToJobId(policyName)))
    }
}
