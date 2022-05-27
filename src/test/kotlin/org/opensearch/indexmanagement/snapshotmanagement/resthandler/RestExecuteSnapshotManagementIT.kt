/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.resthandler

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.SM_POLICIES_URI
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.rest.RestStatus
import java.time.Instant

class RestExecuteSnapshotManagementIT : SnapshotManagementRestTestCase() {

    @Suppress("UNCHECKED_CAST")
    fun `test executing a snapshot management policy`() {
        var smPolicy = createSMPolicy(randomSMPolicy())
        val response = client().makeRequest("PUT", "$SM_POLICIES_URI/${smPolicy.policyName}/_execute")
        assertEquals("Create SM policy failed", RestStatus.OK, response.restStatus())
        val responseBody = response.asMap()
        assertTrue("Response is missing policy", responseBody.containsKey(SMPolicy.SM_TYPE))
        val policyJson = responseBody[SMPolicy.SM_TYPE] as Map<String, Any>
        smPolicy = smPolicy.copy(
            jobEnabledTime = (policyJson[SMPolicy.ENABLED_TIME_FIELD] as Long?)?.let { Instant.ofEpochMilli(it) },
            jobLastUpdateTime = Instant.ofEpochMilli(policyJson[SMPolicy.LAST_UPDATED_TIME_FIELD] as Long)
        )
        assertEquals("The returned policy differed from the actual policy", policyJson, smPolicy.convertToMap()[SMPolicy.SM_TYPE])
    }
}
