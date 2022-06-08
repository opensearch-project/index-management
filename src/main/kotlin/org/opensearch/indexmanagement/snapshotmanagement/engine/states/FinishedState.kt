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
import org.opensearch.indexmanagement.snapshotmanagement.updateNextExecutionTime
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
                return SMResult.Fail(metadataBuilder.build(), WorkflowType.CREATION)
            metadataBuilder.resetRetry(creation = true)
            val getSnapshots = getSnapshotsRes.snapshots

            if (getSnapshots.isEmpty()) {
                // probably user deletes the creating snapshot
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.SUCCESS,
                    message = getSnapshotMissingMessageInCreationWorkflow(started),
                    endTime = now(),
                ).resetWorkflow()
                return@let
            }
            val snapshot = getSnapshots.first()
            when (snapshot.state()) {
                SnapshotState.SUCCESS -> {
                    metadataBuilder.setLatestExecution(
                        status = SMMetadata.LatestExecution.Status.SUCCESS,
                        message = "Snapshot ${metadata.creation.started.first()} creation end with state ${snapshot.state()}.",
                        endTime = now(),
                    ).setCreationStarted(null)
                    // TODO SM notification snapshot created
                }
                SnapshotState.IN_PROGRESS -> {
                    job.creation.timeLimit?.let { timeLimit ->
                        if (timeLimit.isExceed(metadata.creation.latestExecution!!.startTime)) {
                            log.warn(getTimeLimitExceedMessage(job.creation.timeLimit))
                            metadataBuilder.setLatestExecution(
                                status = SMMetadata.LatestExecution.Status.TIME_LIMIT_EXCEEDED,
                                cause = getTimeLimitExceedMessage(job.creation.timeLimit),
                                endTime = now(),
                            )
                            return SMResult.Fail(metadataBuilder.build(), WorkflowType.CREATION, forceReset = true)
                        }
                    }
                }
                else -> {
                    // FAILED, PARTIAL, INCOMPATIBLE
                    metadataBuilder.setLatestExecution(
                        status = SMMetadata.LatestExecution.Status.FAILED,
                        cause = "Snapshot ${metadata.creation.started.first()} creation end with state ${snapshot.state()}.",
                        endTime = now(),
                    ).setCreationStarted(null)
                    // TODO SM notification snapshot creation has problem
                }
            }

            // TODO SM notification: if now is after next creation time, update nextCreationTime to next execution schedule
            //  and try notify user that we skip the execution because snapshot creation time
            //  is longer than execution schedule
            val result = updateNextExecutionTime(metadataBuilder, metadata.creation.trigger.time, job.creation.schedule, WorkflowType.CREATION, log)
            if (result.updated) metadataBuilder = result.metadataBuilder
        }

        metadata.deletion?.started?.let { startedDeleteSnapshots ->
            assert(metadata.deletion.latestExecution != null)
            metadataBuilder.workflow(WorkflowType.DELETION)

            if (job.deletion == null)
                return SMResult.Fail(metadataBuilder.build(), WorkflowType.DELETION, forceReset = true)

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
                return SMResult.Fail(metadataBuilder.build(), WorkflowType.DELETION)
            metadataBuilder.resetRetry(deletion = true)
            val getSnapshots = getSnapshotsRes.snapshots

            val existingSnapshotsNameSet = getSnapshots.map { it.snapshotId().name }.toSet()
            val remainingSnapshotsName = existingSnapshotsNameSet intersect startedDeleteSnapshots.toSet()
            if (remainingSnapshotsName.isEmpty()) {
                // TODO SM notification snapshot deleted
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.SUCCESS,
                    message = "Snapshots ${metadata.deletion.started} deletion has finished.",
                    endTime = now(),
                ).setDeletionStarted(null)
            } else {
                job.deletion.timeLimit?.let { timeLimit ->
                    if (timeLimit.isExceed(metadata.deletion.latestExecution!!.startTime)) {
                        log.warn(getTimeLimitExceedMessage(job.deletion.timeLimit))
                        metadataBuilder.setLatestExecution(
                            status = SMMetadata.LatestExecution.Status.TIME_LIMIT_EXCEEDED,
                            cause = getTimeLimitExceedMessage(job.deletion.timeLimit),
                            endTime = now(),
                        )
                        return SMResult.Fail(metadataBuilder.build(), WorkflowType.DELETION, forceReset = true)
                    }
                }

                log.info("Snapshots haven't been deleted: $remainingSnapshotsName.")
                metadataBuilder.setDeletionStarted(
                    remainingSnapshotsName.toList(),
                )
            }

            // TODO SM notification: if now is after next deletion time, we can update nextDeletionTime and try notify user
            val result = updateNextExecutionTime(metadataBuilder, metadata.deletion.trigger.time, job.deletion.schedule, WorkflowType.DELETION, log)
            if (result.updated) metadataBuilder = result.metadataBuilder
        }

        val metadataToSave = metadataBuilder.build()
        if (metadataToSave.creation.started != null || metadataToSave.deletion?.started != null) {
            return SMResult.Stay(metadataToSave)
        }
        return SMResult.Next(metadataToSave)
    }

    private fun getSnapshotMissingMessageInCreationWorkflow(snapshot: String) = "Snapshot $snapshot not found while checking if it has been created."
    private fun getSnapshotExceptionInCreationWorkflow(snapshot: String) = "Caught exception while getting started creation snapshot [$snapshot]."
    private fun getSnapshotMissingMessageInDeletionWorkflow() = "No snapshots found under policy while getting snapshots to decide if snapshots has been deleted."
    private fun getSnapshotExceptionInDeletionWorkflow(startedDeleteSnapshots: List<String>) = "Caught exception while getting snapshots to decide if snapshots [$startedDeleteSnapshots] has been deleted."
    private fun getTimeLimitExceedMessage(timeLimit: TimeValue) = "Time limit $timeLimit exceeded."

    private fun TimeValue.isExceed(startTime: Instant): Boolean {
        return (now().toEpochMilli() - startTime.toEpochMilli()) > this.millis
    }
}
