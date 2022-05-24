/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.resthandler

import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementRestTestCase
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy

class RestExplainSnapshotManagementIT : SnapshotManagementRestTestCase() {

    fun `test explaining a snapshot management policy`() {
        val smPolicy = createSMPolicy(randomSMPolicy().copy(jobEnabled = false, jobEnabledTime = null))
//        val smMetadata
    }
}
