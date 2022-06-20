/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import org.apache.logging.log4j.Logger
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.transport.RemoteTransportException
import java.time.Instant
import java.time.Instant.now

object DeletingState : State {

    override val continuous: Boolean = false

    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.DELETION)

        if (job.deletion == null) {
            log.warn("Policy deletion config becomes null before trying to delete old snapshots. Reset.")
            return SMResult.Fail(
                metadataBuilder.resetDeletion(), WorkflowType.DELETION, forceReset = true
            )
        }

        val snapshotsToDelete: List<String>

        val getSnapshotsRes = client.getSnapshots(
            job, smDocIdToPolicyName(job.id) + "*", metadataBuilder, log,
            getSnapshotsMissingMessage(),
            getSnapshotsErrorMessage(),
        )
        metadataBuilder = getSnapshotsRes.metadataBuilder
        if (getSnapshotsRes.failed)
            return SMResult.Fail(metadataBuilder, WorkflowType.DELETION)
        val getSnapshots = getSnapshotsRes.snapshots
        metadataBuilder.resetRetry()

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
                val res: AcknowledgedResponse = client.admin().cluster().suspendUntil { deleteSnapshot(req, it) }
                log.info("sm dev: Delete snapshot acknowledged: ${res.isAcknowledged}.")
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.IN_PROGRESS
                ).setDeletionStarted(snapshotsToDelete)
            } catch (ex: RemoteTransportException) {
                val unwrappedException = ExceptionsHelper.unwrapCause(ex) as Exception
                return handleException(unwrappedException, snapshotsToDelete, metadataBuilder, log)
            } catch (ex: Exception) {
                return handleException(ex, snapshotsToDelete, metadataBuilder, log)
            }
        }
        return SMResult.Next(metadataBuilder)
    }

    private fun handleException(ex: Exception, snapshotsToDelete: List<String>, metadataBuilder: SMMetadata.Builder, log: Logger): SMResult {
        log.error(getDeleteSnapshotErrorMessage(snapshotsToDelete), ex)
        metadataBuilder.setLatestExecution(
            status = SMMetadata.LatestExecution.Status.RETRYING,
            message = getDeleteSnapshotErrorMessage(snapshotsToDelete),
            cause = ex,
        )
        return SMResult.Fail(metadataBuilder, WorkflowType.CREATION)
    }

    private fun getSnapshotsMissingMessage() = "No snapshots found under policy while getting snapshots to decide which snapshots to delete."
    private fun getSnapshotsErrorMessage() = "Caught exception while getting snapshots to decide which snapshots to delete."
    private fun getDeleteSnapshotErrorMessage(snapshotNames: List<String>) = "Caught exception while deleting snapshot $snapshotNames."

    /**
     * Based on delete conditions, find which snapshots to be deleted
     *
     * Logic:
     *   snapshots older than max_age: snapshot.startedTime is before (now - max_age)
     *   keep at least min_count of snapshots even it's outdated
     *   keep at most max_count of snapshots even it's not outdated
     */
    private fun filterByDeleteCondition(snapshots: List<SnapshotInfo>, deleteCondition: SMPolicy.DeleteCondition, log: Logger): List<String> {
        log.info("sm dev: snapshotInfos $snapshots")
        // sorted in startTime ascending order
        val snapshotInfos = snapshots.sortedBy { it.startTime() } // start_time will always exist along with snapshotId
        log.info("sm dev: snapshotInfos $snapshotInfos")

        var thresholdCount = 0

        if (deleteCondition.maxAge != null) {
            val timeThreshold = now().minusSeconds(deleteCondition.maxAge.seconds())
            log.info("sm dev: time threshold: $timeThreshold")
            val thresholdSnapshot = snapshotInfos.findLast { Instant.ofEpochMilli(it.startTime()).isBefore(timeThreshold) }
            log.info("sm dev: thresholdSnapshot: $thresholdSnapshot")
            // how many snapshot from beginning satisfy the deletion condition
            thresholdCount = snapshotInfos.indexOf(thresholdSnapshot) + 1
            log.info("sm dev: thresholdCount: $thresholdCount")
            val minCount = deleteCondition.minCount
            if (snapshotInfos.size - thresholdCount < minCount) {
                thresholdCount = offSetThresholdCount(snapshotInfos.size - minCount)
            }
        }

        deleteCondition.maxCount?.let {
            if (snapshotInfos.size - thresholdCount > deleteCondition.maxCount) {
                thresholdCount = snapshotInfos.size - deleteCondition.maxCount
            }
        }

        return snapshotInfos.subList(0, thresholdCount).map { it.snapshotId().name }
    }

    private fun offSetThresholdCount(count: Int): Int {
        if (count < 0) return 0
        return count
    }
}
