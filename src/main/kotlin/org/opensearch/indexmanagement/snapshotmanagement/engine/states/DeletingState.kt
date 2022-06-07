/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementException
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshotsWithErrorHandling
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.snapshots.SnapshotInfo
import java.time.Instant
import java.time.Instant.now

object DeletingState : State {

    override val continuous: Boolean = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.DELETION)

        val res: AcknowledgedResponse
        val snapshotsToDelete: List<String>

        val getSnapshotsRes = client.getSnapshotsWithErrorHandling(
            job,
            smDocIdToPolicyName(job.id) + "*",
            metadataBuilder,
            log,
            getSnapshotsMissingMessage(),
            getSnapshotsErrorMessage(),
        )
        metadataBuilder = getSnapshotsRes.metadataBuilder
        if (getSnapshotsRes.failed)
            return SMResult.Fail(metadataBuilder.build(), WorkflowType.DELETION)
        metadataBuilder.resetRetry(deletion = true)
        val getSnapshots = getSnapshotsRes.snapshots
        log.info("sm dev get snapshots $getSnapshots")

        snapshotsToDelete = filterByDeleteCondition(
            getSnapshots, // TODO SM filter to only useful snapshots: like not FAILED
            job.deletion.condition, log
        )
        log.info("sm dev: Going to delete: $snapshotsToDelete")
        if (snapshotsToDelete.isNotEmpty()) {
            try {
                val req = DeleteSnapshotRequest(
                    job.snapshotConfig["repository"] as String,
                    *snapshotsToDelete.toTypedArray()
                )
                res = client.admin().cluster().suspendUntil { deleteSnapshot(req, it) }
                log.info("sm dev: Delete snapshot acknowledged: ${res.isAcknowledged}.")
            } catch (ex: Exception) {
                log.error(getDeleteSnapshotErrorMessage(snapshotsToDelete), ex)
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.RETRYING,
                    message = getDeleteSnapshotErrorMessage(snapshotsToDelete),
                    cause = SnapshotManagementException.wrap(ex).message,
                )
                return SMResult.Fail(metadataBuilder.build(), WorkflowType.DELETION)
            }
            metadataBuilder.resetRetry(deletion = true)
        }

        if (snapshotsToDelete.isNotEmpty()) {
            metadataBuilder.setLatestExecution(
                status = SMMetadata.LatestExecution.Status.IN_PROGRESS
            ).setDeletionStarted(snapshotsToDelete)
        }
        return SMResult.Next(metadataBuilder.build())
    }

    private fun getSnapshotsMissingMessage() = "No snapshots found under policy while getting snapshots to decide which snapshots to delete."
    private fun getSnapshotsErrorMessage() = "Caught exception while getting snapshots to decide which snapshots to delete."
    private fun getDeleteSnapshotErrorMessage(snapshotNames: List<String>) = "Caught exception while deleting snapshot $snapshotNames."

    /**
     * Based on the condition to delete, find snapshots to be deleted
     *
     * Logic:
     *   outdated snapshots: snapshot.startedTime < now - max_age
     *   keep at least min_count of snapshots even it's outdated
     *   keep at most max_count of snapshots
     */
    private fun filterByDeleteCondition(snapshots: List<SnapshotInfo>, deleteCondition: SMPolicy.DeleteCondition, log: Logger): List<String> {
        log.info("sm dev: snapshotInfos $snapshots")
        val snapshotInfos = snapshots.sortedBy { it.startTime() } // start_time will always exist along with snapshotId
        log.info("sm dev: snapshotInfos $snapshotInfos")

        var thresholdCount = 0

        if (deleteCondition.maxAge != null) {
            val timeThreshold = now().minusSeconds(deleteCondition.maxAge.seconds())
            log.info("sm dev: time threshold: $timeThreshold")
            val thresholdSnapshot = snapshotInfos.findLast { Instant.ofEpochMilli(it.startTime()).isBefore(timeThreshold) }
            log.info("sm dev: thresholdSnapshot: $thresholdSnapshot")
            thresholdCount = snapshotInfos.indexOf(thresholdSnapshot) + 1
            log.info("sm dev: thresholdCount: $thresholdCount")
            val minCount = deleteCondition.minCount ?: SMPolicy.DeleteCondition.DEFAULT_MIN_COUNT
            if (snapshotInfos.size - thresholdCount < minCount) {
                thresholdCount = snapshotInfos.size - minCount
            }
        }

        if (snapshotInfos.size - thresholdCount > deleteCondition.maxCount) {
            thresholdCount = snapshotInfos.size - deleteCondition.maxCount
        }

        return snapshotInfos.subList(0, thresholdCount).map { it.snapshotId().name }
    }
}
