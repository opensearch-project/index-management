/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.isExceed
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.timeLimitExceeded
import org.opensearch.indexmanagement.snapshotmanagement.tryUpdatingNextExecutionTime
import java.time.Instant.now

object DeletionFinishedState : State {

    override val continuous = true

    @Suppress("ReturnCount", "NestedBlockDepth")
    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.DELETION)

        metadata.deletion?.started?.let { snapshotsStartedDeletion ->
            if (metadata.deletion.latestExecution == null) {
                // This should not happen
                log.error("latest_execution is null while checking if snapshots [$snapshotsStartedDeletion] deletion has finished. Reset.")
                metadataBuilder.resetWorkflow()
                return@let
            }

            val getSnapshotsRes = client.getSnapshots(
                job, "${job.policyName}*", metadataBuilder, log,
                getSnapshotMissingMessageInDeletionWorkflow(),
                getSnapshotExceptionInDeletionWorkflow(snapshotsStartedDeletion),
            )
            metadataBuilder = getSnapshotsRes.metadataBuilder
            if (getSnapshotsRes.failed)
                return SMResult.Fail(metadataBuilder, WorkflowType.DELETION)
            val getSnapshots = getSnapshotsRes.snapshots

            val existingSnapshotsNameSet = getSnapshots.map { it.snapshotId().name }.toSet()
            val remainingSnapshotsName = existingSnapshotsNameSet intersect snapshotsStartedDeletion.toSet()
            if (remainingSnapshotsName.isEmpty()) {
                val deletionMessage = "Snapshot(s) $snapshotsStartedDeletion deletion has finished."
                job.notificationConfig?.sendDeletionNotification(client, job.policyName, deletionMessage, job.user, log)
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.SUCCESS,
                    message = deletionMessage,
                    endTime = now(),
                ).setDeletionStarted(null)
            } else {
                job.deletion?.timeLimit?.let { timeLimit ->
                    if (timeLimit.isExceed(metadata.deletion.latestExecution.startTime)) {
                        return timeLimitExceeded(timeLimit, metadataBuilder, WorkflowType.DELETION, log)
                    }
                }

                log.info("Retention snapshots haven't been deleted: $remainingSnapshotsName.")
            }

            // if now is after next deletion time, update next execution schedule
            // TODO may want to notify user that we skipped the execution because snapshot deletion time is longer than execution schedule
            job.deletion?.let {
                val result = tryUpdatingNextExecutionTime(
                    metadataBuilder, metadata.deletion.trigger.time, job.deletion.schedule, WorkflowType.DELETION, log
                )
                if (result.updated) {
                    metadataBuilder = result.metadataBuilder
                }
            }
        }

        val metadataToSave = metadataBuilder.build()
        if (metadataToSave.deletion?.started != null) {
            return SMResult.Stay(metadataBuilder)
        }
        return SMResult.Next(metadataBuilder)
    }

    private fun getSnapshotMissingMessageInDeletionWorkflow() =
        "No snapshots found under policy while getting snapshots to decide if snapshots has been deleted."
    private fun getSnapshotExceptionInDeletionWorkflow(startedDeleteSnapshots: List<String>) =
        "Caught exception while getting snapshots to decide if snapshots [$startedDeleteSnapshots] has been deleted."
}
