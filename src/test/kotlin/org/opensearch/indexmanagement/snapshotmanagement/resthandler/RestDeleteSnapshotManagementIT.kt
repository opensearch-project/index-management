/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.resthandler

import org.opensearch.client.ResponseException
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.rest.RestStatus

class RestDeleteSnapshotManagementIT : SnapshotManagementRestTestCase() {

    fun `test deleting a snapshot management policy`() {
        val smPolicy = createSMPolicy(randomSMPolicy())
        val deleteResponse = client().makeRequest("DELETE", "${IndexManagementPlugin.SM_POLICIES_URI}/${smPolicy.policyName}?refresh=true")
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        try {
            client().makeRequest("GET", "${IndexManagementPlugin.SM_POLICIES_URI}/${smPolicy.policyName}")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test deleting a snapshot management policy that doesn't exist in existing config index`() {
        try {
            createSMPolicy(randomSMPolicy())
            client().makeRequest("DELETE", "${IndexManagementPlugin.SM_POLICIES_URI}/nonexistent_policy")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test deleting a snapshot management policy that doesn't exist and config index doesnt exist`() {
        try {
            deleteIndex(INDEX_MANAGEMENT_INDEX)
            client().makeRequest("DELETE", "${IndexManagementPlugin.SM_POLICIES_URI}/nonexistent_policy")
            fail("expected 404 ResponseException")
        } catch (e: ResponseException) {
            assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
