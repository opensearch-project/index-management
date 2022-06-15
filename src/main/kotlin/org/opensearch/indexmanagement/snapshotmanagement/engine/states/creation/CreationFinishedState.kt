/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation

import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshotsWithErrorHandling
import org.opensearch.indexmanagement.snapshotmanagement.isExceed
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.timeLimitExceeded
import org.opensearch.indexmanagement.snapshotmanagement.updateNextExecutionTime
import org.opensearch.snapshots.SnapshotState
import java.time.Instant

object CreationFinishedState : State {
    override val continuous = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.CREATION)

        metadata.creation.started?.first()?.let { started ->
            assert(metadata.creation.latestExecution != null)

            val getSnapshotsResult = client.getSnapshotsWithErrorHandling(
                job, started, metadataBuilder, log,
                getSnapshotMissingMessageInCreationWorkflow(started),
                getSnapshotExceptionInCreationWorkflow(started),
            )
            log.info("sm dev get snapshots result $getSnapshotsResult")
            metadataBuilder = getSnapshotsResult.metadataBuilder
            if (getSnapshotsResult.failed)
                return SMResult.Fail(metadataBuilder, WorkflowType.CREATION)
            metadataBuilder.resetRetry(creation = true)
            val getSnapshots = getSnapshotsResult.snapshots

            if (getSnapshots.isEmpty()) {
                // probably user deletes the creating snapshot
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.SUCCESS,
                    message = getSnapshotMissingMessageInCreationWorkflow(started),
                    endTime = Instant.now(),
                ).resetWorkflow()
                return@let
            }
            val snapshot = getSnapshots.first()
            when (snapshot.state()) {
                SnapshotState.SUCCESS -> {
                    metadataBuilder.setLatestExecution(
                        status = SMMetadata.LatestExecution.Status.SUCCESS,
                        message = "Snapshot ${metadata.creation.started.first()} creation end with state ${snapshot.state()}.",
                        endTime = Instant.now(),
                    ).setCreationStarted(null)
                    // TODO SM notification snapshot created
                }
                SnapshotState.IN_PROGRESS -> {
                    job.creation.timeLimit?.let { timeLimit ->
                        if (timeLimit.isExceed(metadata.creation.latestExecution?.startTime)) {
                            return timeLimitExceeded(
                                timeLimit,
                                metadataBuilder,
                                WorkflowType.CREATION,
                                log
                            )
                        }
                    }
                }
                else -> {
                    // FAILED, PARTIAL, INCOMPATIBLE
                    metadataBuilder.setLatestExecution(
                        status = SMMetadata.LatestExecution.Status.FAILED,
                        cause = "Snapshot ${metadata.creation.started.first()} creation end with state ${snapshot.state()}.",
                        endTime = Instant.now(),
                    ).setCreationStarted(null)
                    // TODO SM notification snapshot creation has problem
                }
            }

            // TODO SM notification: if now is after next creation time, update nextCreationTime to next execution schedule
            //  and try notify user that we skip the execution because snapshot creation time
            //  is longer than execution schedule
            val result = updateNextExecutionTime(
                metadataBuilder, metadata.creation.trigger.time, job.creation.schedule,
                WorkflowType.CREATION, log
            )
            if (result.updated) metadataBuilder = result.metadataBuilder
        }

        val metadataToSave = metadataBuilder.build()
        if (metadataToSave.creation.started != null) {
            return SMResult.Stay(metadataBuilder)
        }
        return SMResult.Next(metadataBuilder)
    }

    private fun getSnapshotMissingMessageInCreationWorkflow(snapshot: String) = "Snapshot $snapshot not found while checking if it has been created."
    private fun getSnapshotExceptionInCreationWorkflow(snapshot: String) = "Caught exception while getting started creation snapshot [$snapshot]."
}
