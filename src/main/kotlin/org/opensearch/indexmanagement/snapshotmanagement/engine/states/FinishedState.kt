/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshotsWithErrorHandling
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.snapshots.SnapshotState
import java.time.Instant
import java.time.Instant.now

object FinishedState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)

        metadata.creation.started?.first()?.let { started ->
            assert(metadata.creation.latestExecution != null)
            metadataBuilder.workflow(WorkflowType.CREATION)

            val getSnapshotsRes = client.getSnapshotsWithErrorHandling(
                job,
                started,
                metadataBuilder,
                log,
                getSnapshotMissingMessageInCreationWorkflow(started),
                getSnapshotExceptionInCreationWorkflow(started),
            )
            metadataBuilder = getSnapshotsRes.metadataBuilder
            if (getSnapshotsRes.failed)
                return SMResult.Failure(metadataBuilder.build(), WorkflowType.CREATION)
            metadataBuilder.resetRetry(creation = true)
            val getSnapshots = getSnapshotsRes.snapshots

            if (getSnapshots.isEmpty()) {
                metadataBuilder
                    .setLatestExecution(
                        SMMetadata.LatestExecution.Status.FAILED,
                        message = getSnapshotMissingMessageInCreationWorkflow(started),
                        endTime = now(),
                    )
                    .resetWorkflow()
                return@let
            }
            val snapshot = getSnapshots.first()
            when (snapshot.state()) {
                SnapshotState.SUCCESS -> {
                    metadataBuilder
                        .setCreation(snapshot = null)
                        .setLatestExecution(
                            status = SMMetadata.LatestExecution.Status.SUCCESS,
                            message = "Snapshot ${metadata.creation.started.first()} creation end with state ${snapshot.state()}.",
                            endTime = now(),
                        )
                    // TODO SM notification snapshot created
                }
                SnapshotState.IN_PROGRESS -> {
                    job.creation.timeLimit?.let {
                        if (timeLimitExceed(metadata.creation.latestExecution!!.startTime, it))
                            return SMResult.TimeLimitExceed(metadataBuilder.build(), WorkflowType.CREATION)
                    }
                }
                else -> {
                    // FAILED, PARTIAL, INCOMPATIBLE
                    metadataBuilder
                        .setCreation(null)
                        .setLatestExecution(
                            status = SMMetadata.LatestExecution.Status.FAILED,
                            cause = "Snapshot ${metadata.creation.started.first()} creation end with state ${snapshot.state()}.",
                            endTime = now(),
                        )
                    // TODO SM notification snapshot creation has problem
                }
            }

            // TODO SM if now is after next creation time, update nextCreationTime to next execution schedule
            //  and try notify user that we skip the execution because snapshot creation time
            //  is longer than execution schedule
        }

        metadata.deletion.started?.let { startedDeleteSnapshots ->
            assert(metadata.deletion.latestExecution != null)
            metadataBuilder.workflow(WorkflowType.DELETION)

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
                return SMResult.Failure(metadataBuilder.build(), WorkflowType.DELETION)
            metadataBuilder.resetRetry(deletion = true)
            val getSnapshots = getSnapshotsRes.snapshots

            if (getSnapshots.isEmpty()) {
                metadataBuilder.setLatestExecution(
                    SMMetadata.LatestExecution.Status.FAILED,
                    message = getSnapshotMissingMessageInDeletionWorkflow(),
                    endTime = now(),
                )
                    .resetWorkflow()
            }

            val existingSnapshotsNameSet = getSnapshots.map { it.snapshotId().name }.toSet()
            val remainingSnapshotsName = existingSnapshotsNameSet intersect startedDeleteSnapshots.toSet()
            if (remainingSnapshotsName.isEmpty()) {
                log.info("Snapshots have been deleted: $existingSnapshotsNameSet.")
                // TODO SM notification snapshot deleted
                metadataBuilder.setDeletion(null)
                    .setLatestExecution(
                        status = SMMetadata.LatestExecution.Status.SUCCESS,
                        message = "Snapshots ${metadata.creation.started} deletion has finished.",
                        endTime = now(),
                    )
            } else {
                job.deletion.timeLimit?.let {
                    if (timeLimitExceed(metadata.deletion.latestExecution!!.startTime, it))
                        return SMResult.TimeLimitExceed(metadataBuilder.build(), WorkflowType.DELETION)
                }

                log.info("Snapshots haven't been deleted: $remainingSnapshotsName.")
                metadataBuilder.setDeletion(
                    remainingSnapshotsName.toList(),
                )
            }

            // TODO SM if now is after next deletion time, we can update nextDeletionTime and try notify user
        }

        val metadataToSave = metadataBuilder.build()
        if (metadataToSave.creation.started != null || metadataToSave.deletion.started != null) {
            return SMResult.Stay(metadataToSave)
        }
        return SMResult.Next(metadataToSave)
    }

    private fun getSnapshotMissingMessageInCreationWorkflow(snapshot: String) = "Snapshot $snapshot not found while checking if it has been created."
    private fun getSnapshotExceptionInCreationWorkflow(snapshot: String) = "Caught exception while getting started creation snapshot [$snapshot]."
    private fun getSnapshotMissingMessageInDeletionWorkflow() = "No snapshots found under policy while getting snapshots to decide if snapshots has been deleted."
    private fun getSnapshotExceptionInDeletionWorkflow(startedDeleteSnapshots: List<String>) = "Caught exception while getting snapshots to decide if snapshots [$startedDeleteSnapshots] has been deleted."

    private fun timeLimitExceed(startTime: Instant, timeLimit: TimeValue): Boolean {
        return (now().toEpochMilli() - startTime.toEpochMilli()) > timeLimit.millis
    }
}
