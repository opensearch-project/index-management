/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SnapshotManagementException
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SnapshotManagementException.ExceptionCode.API_CALLING
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.startTransaction

object DeletingState : State {

    override val continuous: Boolean = true

    override suspend fun execute(context: SMStateMachine): ExecutionResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        if (metadata.apiCalling) {
            return ExecutionResult.Failure(SnapshotManagementException(API_CALLING))
        }

        context.startTransaction()

        val res: AcknowledgedResponse
        val snapshotToDelete: List<String>
        try {
            val getStatusReq = SnapshotsStatusRequest()
                .snapshots(arrayOf(job.policyName))
                .repository(job.snapshotConfig["repository"] as String)
            val snapshotStatusRes: SnapshotsStatusResponse = client.admin().cluster().suspendUntil { snapshotsStatus(getStatusReq, it) }
            log.info("Get snapshot status: ${snapshotStatusRes.snapshots}")
            snapshotToDelete = findSnapshotsToDelete(snapshotStatusRes)

            val req = DeleteSnapshotRequest(
                job.snapshotConfig["repository"] as String,
                *snapshotToDelete.toTypedArray()
            )
            res = client.admin().cluster().suspendUntil { deleteSnapshot(req, it) }
        } catch (ex: Exception) {
            return ExecutionResult.Failure(ex)
        }

        log.info("Delete snapshot acknowledged: ${res.isAcknowledged}.")
        context.metadataToSave = metadata.copy(
            currentState = SMState.CREATING,
            apiCalling = false,
            deletion = metadata.deletion.copy(
                started = snapshotToDelete
            ),
        )
        return ExecutionResult.Next
    }

    private fun findSnapshotsToDelete(res: SnapshotsStatusResponse): List<String> {
        val res = emptyList<String>()

        // get a map of snapshot name to endtime
        // at least keep 5, at most keep 50
        // 5-50 check age
        // based on delete condition, age condition surpassed snapshots

        return res
    }
}
