/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport

import org.opensearch.action.ActionType
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.TransportIndexSMAction

object SMActions {
    /**
     * [TransportIndexSMAction]
     */
    const val INDEX_SM_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/put"
    val INDEX_SM_ACTION_TYPE = ActionType(INDEX_SM_ACTION_NAME, ::IndexSMResponse)
}
