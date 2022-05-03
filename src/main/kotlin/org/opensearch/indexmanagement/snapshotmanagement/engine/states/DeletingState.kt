/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.StateMachineException
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.StateMachineException.ExceptionCode.ATOMIC
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.startTransaction
import org.opensearch.snapshots.SnapshotInfo
import java.time.Instant
import java.time.Instant.now

object DeletingState : State {

    override val continuous: Boolean = true

    override suspend fun execute(context: SMStateMachine): ExecutionResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        if (metadata.atomic) {
            return ExecutionResult.Failure(StateMachineException(ATOMIC))
        }

        context.startTransaction()

        val res: AcknowledgedResponse
        val snapshotToDelete: List<SMMetadata.SnapshotInfo>
        try {
            val getSnapshotsReq = GetSnapshotsRequest()
                .snapshots(arrayOf(job.policyName))
                .repository(job.snapshotConfig["repository"] as String)
            val getSnapshotsRes: GetSnapshotsResponse = client.admin().cluster().suspendUntil { getSnapshots(getSnapshotsReq, it) }
            log.info("Get snapshot response: ${getSnapshotsRes.snapshots}")
            snapshotToDelete = findSnapshotsToDelete(getSnapshotsRes.snapshots, job.deletion.condition)

            val req = DeleteSnapshotRequest(
                job.snapshotConfig["repository"] as String,
                *snapshotToDelete.map { it.name }.toTypedArray()
            )
            res = client.admin().cluster().suspendUntil { deleteSnapshot(req, it) }
        } catch (ex: Exception) {
            return ExecutionResult.Failure(ex)
        }

        log.info("Delete snapshot acknowledged: ${res.isAcknowledged}.")
        val metadataToSave = metadata.copy(
            currentState = SMState.DELETING,
            atomic = false,
            deletion = metadata.deletion.copy(
                started = snapshotToDelete,
                startedTime = now(),
            ),
        )
        return ExecutionResult.Next(metadataToSave)
    }

    private fun findSnapshotsToDelete(snapshots: List<SnapshotInfo>, deleteCondition: SMPolicy.DeleteCondition): List<SMMetadata.SnapshotInfo> {
        val snapshotInfos = snapshots.map {
            SMMetadata.SnapshotInfo(
                it.snapshotId().name,
                Instant.ofEpochMilli(it.startTime()),
                Instant.ofEpochMilli(it.endTime()),
            )
        }.sortedBy { it.startTime } // start_time will always exist along with snapshotId

        var thresholdIndex = 0
        if (deleteCondition.maxAge != null) {
            val timeThreshold = now().minusSeconds(deleteCondition.maxAge.seconds())
            val thresholdSnapshot = snapshotInfos.findLast { it.startTime?.isBefore(timeThreshold) ?: false }
            thresholdIndex = snapshotInfos.indexOf(thresholdSnapshot)
            if (thresholdIndex == -1) thresholdIndex = 0
            if (snapshotInfos.size - thresholdIndex < deleteCondition.minCount!!) {
                thresholdIndex = snapshotInfos.size - deleteCondition.minCount
            }
        }
        if (snapshotInfos.size - thresholdIndex > deleteCondition.maxCount) {
            thresholdIndex = snapshotInfos.size - deleteCondition.maxCount
        }

        return snapshotInfos.subList(0, thresholdIndex)
    }
}
