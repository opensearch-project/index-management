/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.resthandler

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.execute.ExecuteSMResponse.Companion.SNAPSHOT_RESPONSE_FIELD
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.rest.RestStatus

class RestExecuteSnapshotManagementIT : SnapshotManagementRestTestCase() {

    @Suppress("UNCHECKED_CAST")
    fun `test executing a snapshot management policy`() {
        val smPolicy = createSMPolicy(randomSMPolicy())
        val response = client().makeRequest("PUT", "$SM_POLICIES_URI/${smPolicy.policyName}/_execute")
        assertEquals("Execute SM policy failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        assertTrue("Response is missing snapshot response", responseBody.containsKey(SNAPSHOT_RESPONSE_FIELD))
    }
}
