/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshotsWithErrorHandling
import org.opensearch.indexmanagement.snapshotmanagement.isExceed
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.indexmanagement.snapshotmanagement.timeLimitExceeded
import org.opensearch.indexmanagement.snapshotmanagement.updateNextExecutionTime
import java.time.Instant

object DeletionFinishedState : State {
    override val continuous = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.DELETION)

        metadata.deletion?.started?.let { startedDeleteSnapshots ->
            assert(metadata.deletion.latestExecution != null)

            val getSnapshotsRes = client.getSnapshotsWithErrorHandling(
                job,
                "${smDocIdToPolicyName(job.id)}*",
                metadataBuilder,
                log,
                getSnapshotMissingMessageInDeletionWorkflow(),
                getSnapshotExceptionInDeletionWorkflow(startedDeleteSnapshots),
            )
            metadataBuilder = getSnapshotsRes.metadataBuilder
            if (getSnapshotsRes.failed)
                return SMResult.Fail(metadataBuilder, WorkflowType.DELETION)
            metadataBuilder.resetRetry(deletion = true)
            val getSnapshots = getSnapshotsRes.snapshots

            val existingSnapshotsNameSet = getSnapshots.map { it.snapshotId().name }.toSet()
            val remainingSnapshotsName = existingSnapshotsNameSet intersect startedDeleteSnapshots.toSet()
            if (remainingSnapshotsName.isEmpty()) {
                // TODO SM notification snapshot deleted
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.SUCCESS,
                    message = "Snapshots ${metadata.deletion.started} deletion has finished.",
                    endTime = Instant.now(),
                ).setDeletionStarted(null)
            } else {
                job.deletion?.timeLimit?.let { timeLimit ->
                    if (timeLimit.isExceed(metadata.deletion.latestExecution?.startTime)) {
                        return timeLimitExceeded(timeLimit, metadataBuilder, WorkflowType.DELETION, log)
                    }
                }

                log.info("Snapshots haven't been deleted: $remainingSnapshotsName.")
                metadataBuilder.setDeletionStarted(
                    remainingSnapshotsName.toList(),
                )
            }

            // TODO SM notification: if now is after next deletion time, we can update nextDeletionTime and try notify user
            job.deletion?.let {
                val result = updateNextExecutionTime(
                    metadataBuilder, metadata.deletion.trigger.time, job.deletion.schedule,
                    WorkflowType.DELETION, log
                )
                if (result.updated) metadataBuilder = result.metadataBuilder
            }
        }

        val metadataToSave = metadataBuilder.build()
        if (metadataToSave.deletion?.started != null) {
            return SMResult.Stay(metadataBuilder)
        }
        return SMResult.Next(metadataBuilder)
    }

    private fun getSnapshotMissingMessageInDeletionWorkflow() = "No snapshots found under policy while getting snapshots to decide if snapshots has been deleted."
    private fun getSnapshotExceptionInDeletionWorkflow(startedDeleteSnapshots: List<String>) = "Caught exception while getting snapshots to decide if snapshots [$startedDeleteSnapshots] has been deleted."
}
