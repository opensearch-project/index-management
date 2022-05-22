/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.Result
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.smJobIdToPolicyName
import org.opensearch.snapshots.SnapshotInfo
import java.time.Instant
import java.time.Instant.now

object DeletingState : State {

    override val continuous: Boolean = true

    override suspend fun execute(context: SMStateMachine): Result {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        val res: AcknowledgedResponse
        val snapshotToDelete: List<SMMetadata.SnapshotInfo>
        try {
            // TODO SM what if there's no snapshot, will there be exception?
            val snapshotInfos = client.getSnapshots(
                smJobIdToPolicyName(job.id) + "*",
                job.snapshotConfig["repository"] as String
            )
            snapshotToDelete = findSnapshotsToDelete(snapshotInfos, job.deletion.condition)
            log.info("sm dev: Going to delete: ${snapshotToDelete.map { it.name }}")

            if (snapshotToDelete.isNotEmpty()) {
                val req = DeleteSnapshotRequest(
                    job.snapshotConfig["repository"] as String,
                    *snapshotToDelete.map { it.name }.toTypedArray()
                )
                res = client.admin().cluster().suspendUntil { deleteSnapshot(req, it) }
                log.info("sm dev: Delete snapshot acknowledged: ${res.isAcknowledged}.")
            }
        } catch (ex: Exception) {
            return Result.Failure(ex, WorkflowType.DELETION, reset = true)
        }

        val metadataToSave = SMMetadata.Builder(metadata)
            .currentState(SMState.DELETING)
            .deletionStart(
                startTime = now(),
                snapshotInfo = snapshotToDelete
            )
            .build()
        return Result.Next(metadataToSave)
    }

    /**
     * Based on the condition to delete, find snapshots to be deleted
     *
     * Logic:
     *   outdated snapshots: snapshot.startedTime + max_age < now
     *   keep at least min_count of snapshots even it's outdated
     *   keep at most max_count of snapshots
     */
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
            val minCount = deleteCondition.minCount ?: SMPolicy.DeleteCondition.DEFAULT_MIN_COUNT
            if (snapshotInfos.size - thresholdIndex < minCount) {
                thresholdIndex = snapshotInfos.size - minCount
            }
        }

        if (snapshotInfos.size - thresholdIndex > deleteCondition.maxCount) {
            thresholdIndex = snapshotInfos.size - deleteCondition.maxCount
        }

        return snapshotInfos.subList(0, thresholdIndex)
    }
}
