/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport

import org.opensearch.action.ActionType
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete.TransportDeleteSMPolicyAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.TransportGetSMPolicyAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.TransportIndexSMPolicyAction

object SMActions {
    /**
     * [TransportIndexSMPolicyAction]
     */
    const val INDEX_SM_POLICY_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/policy/write"
    val INDEX_SM_POLICY_ACTION_TYPE = ActionType(INDEX_SM_POLICY_ACTION_NAME, ::IndexSMPolicyResponse)

    /**
     * [TransportGetSMPolicyAction]
     */
    const val GET_SM_POLICY_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/policy/get"
    val GET_SM_POLICY_ACTION_TYPE = ActionType(GET_SM_POLICY_ACTION_NAME, ::GetSMPolicyResponse)

    /**
     * [TransportDeleteSMPolicyAction]
     */
    const val DELETE_SM_POLICY_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/policy/delete"
    val DELETE_SM_POLICY_ACTION_TYPE = ActionType(DELETE_SM_POLICY_ACTION_NAME, ::DeleteResponse)
}
