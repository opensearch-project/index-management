/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport

import org.opensearch.action.ActionType
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete.TransportDeleteSMPolicyAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain.ExplainSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPoliciesResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.GetSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.TransportGetSMPolicyAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.IndexSMPolicyResponse
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.index.TransportIndexSMPolicyAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.get.TransportGetSMPoliciesAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain.TransportExplainSMAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.start.TransportStartSMAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.stop.TransportStopSMAction

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
     * [TransportGetSMPoliciesAction]
     */
    const val GET_SM_POLICIES_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/policy/search"
    val GET_SM_POLICIES_ACTION_TYPE = ActionType(GET_SM_POLICIES_ACTION_NAME, ::GetSMPoliciesResponse)

    /**
     * [TransportDeleteSMPolicyAction]
     */
    const val DELETE_SM_POLICY_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/policy/delete"
    val DELETE_SM_POLICY_ACTION_TYPE = ActionType(DELETE_SM_POLICY_ACTION_NAME, ::DeleteResponse)

    /**
     * [TransportExplainSMAction]
     */
    const val EXPLAIN_SM_POLICY_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/policy/explain"
    val EXPLAIN_SM_POLICY_ACTION_TYPE = ActionType(EXPLAIN_SM_POLICY_ACTION_NAME, ::ExplainSMPolicyResponse)

    /**
     * [TransportStartSMAction]
     */
    const val START_SM_POLICY_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/policy/start"
    val START_SM_POLICY_ACTION_TYPE = ActionType(START_SM_POLICY_ACTION_NAME, ::AcknowledgedResponse)

    /**
     * [TransportStopSMAction]
     */
    const val STOP_SM_POLICY_ACTION_NAME = "cluster:admin/opensearch/snapshot_management/policy/stop"
    val STOP_SM_POLICY_ACTION_TYPE = ActionType(STOP_SM_POLICY_ACTION_NAME, ::AcknowledgedResponse)
}
