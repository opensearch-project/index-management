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
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine.Companion.apiCallingMsg
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.StateMachineException
import org.opensearch.indexmanagement.snapshotmanagement.revertTransaction
import org.opensearch.indexmanagement.snapshotmanagement.startTransaction

object DeletingState : State {

    override val continuous: Boolean = true

    override suspend fun execute(context: SMStateMachine): Boolean {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        if (metadata.apiCalling) {
            // probably because we cannot index metadata after transaction
            throw StateMachineException(message = apiCallingMsg())
        }

        context.startTransaction()

        val req = SnapshotsStatusRequest()
            .snapshots(arrayOf(job.policyName))
            .repository(job.snapshotConfig["repository"] as String)
        val snapshotStatusRes: SnapshotsStatusResponse = client.admin().cluster().suspendUntil { snapshotsStatus(req, it) }
        log.info("Get snapshot status: ${snapshotStatusRes.snapshots}")
        val snapshotToDelete = findSnapshotsToDelete(snapshotStatusRes)

        val res: AcknowledgedResponse
        try {
            val req = DeleteSnapshotRequest(
                job.snapshotConfig["repository"] as String,
                *snapshotToDelete.toTypedArray()
            )
            res = client.admin().cluster().suspendUntil { deleteSnapshot(req, it) }
        } catch (ex: Exception) {
            context.revertTransaction()
            throw ex
        }

        return if (res.isAcknowledged) {
            log.info("Delete snapshot acknowledged.")
            context.metadataToSave = metadata.copy(
                currentState = SMState.CREATING,
                apiCalling = false,
                deletion = metadata.deletion.copy(
                    started = snapshotToDelete
                ),
            )
            true
        } else {
            throw StateMachineException("Delete snapshot $snapshotToDelete not acknowledged.")
        }
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
