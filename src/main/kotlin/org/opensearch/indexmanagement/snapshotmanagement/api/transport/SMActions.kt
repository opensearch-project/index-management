/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport

import org.opensearch.action.ActionType
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.TransportGetSMAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.TransportIndexSMAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete.DeleteSMResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete.TransportDeleteSMAction

object SMActions {
    /**
     * [TransportIndexSMAction]
     */
    const val INDEX_SM_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/write"
    val INDEX_SM_ACTION_TYPE = ActionType(INDEX_SM_ACTION_NAME, ::IndexSMResponse)

    /**
     * [TransportGetSMAction]
     */
    const val GET_SM_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/get"
    val GET_SM_ACTION_TYPE = ActionType(GET_SM_ACTION_NAME, ::GetSMResponse)

    /**
     * [TransportDeleteSMAction]
     */
    const val DELETE_SM_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/delete"
    val DELETE_SM_ACTION_TYPE = ActionType(DELETE_SM_ACTION_NAME, ::DeleteSMResponse)
}
