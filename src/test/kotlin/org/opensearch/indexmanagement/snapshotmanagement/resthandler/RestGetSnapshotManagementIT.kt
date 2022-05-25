/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.rest.RestStatus

class RestGetSnapshotManagementIT : SnapshotManagementRestTestCase() {

    fun `test getting a snapshot management policy`() {
        var smPolicy = createSMPolicy(randomSMPolicy().copy(jobEnabled = false, jobEnabledTime = null))
        val indexedSMPolicy = getSMPolicy(smPolicy.policyName)
        // Schema version and last updated time are updated during the creation so we need to update the original too for comparison
        // Job schedule interval will have a dynamic start time
        smPolicy = smPolicy.copy(
            id = indexedSMPolicy.id,
            seqNo = indexedSMPolicy.seqNo,
            primaryTerm = indexedSMPolicy.primaryTerm,
            jobLastUpdateTime = indexedSMPolicy.jobLastUpdateTime,
            jobSchedule = indexedSMPolicy.jobSchedule
        )
        assertEquals("Indexed and retrieved snapshot management policies differ", smPolicy, indexedSMPolicy)
    }

    @Throws(Exception::class)
    fun `test getting a snapshot management policy that doesn't exist`() {
        try {
            getSMPolicy(randomAlphaOfLength(20))
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    @Throws(Exception::class)
    fun `test getting a snapshot management policy that doesn't exist and config index doesnt exist`() {
        try {
            deleteIndex(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
            getSMPolicy(randomAlphaOfLength(20))
            fail("expected response exception")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
