/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.upsert
import org.opensearch.indexmanagement.snapshotmanagement.smJobIdToPolicyName
import org.opensearch.snapshots.SnapshotMissingException
import org.opensearch.snapshots.SnapshotState
import java.time.Instant
import java.time.Instant.now

object FinishedState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): ExecutionResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var creationStarted = metadata.creation.started
        var deletionStarted = metadata.deletion.started
        var info = metadata.info
        when {
            metadata.creation.started != null -> {
                val snapshots = try {
                    client.getSnapshots(
                        metadata.creation.started.name,
                        job.snapshotConfig["repository"] as String
                    )
                } catch (ex: SnapshotMissingException) {
                    // User may manually delete the creating snapshot
                    return ExecutionResult.Failure(ex, WorkflowType.CREATION, reset = true)
                } catch (ex: Exception) {
                    // TODO SM need to implement retry mechanism so we don't stuck forever
                    return ExecutionResult.Failure(ex, WorkflowType.CREATION, reset = false)
                }

                when (snapshots.firstOrNull()?.state()) {
                    SnapshotState.SUCCESS -> {
                        creationStarted = null
                        info = info.upsert(
                            "last_success" to "${metadata.creation.started} has been created."
                        )
                    }
                    else -> {
                        // IN_PROGRESS, FAILED, PARTIAL, INCOMPATIBLE
                        log.info("Creating snapshot [${metadata.creation.started}] has not succeed")
                        // TODO SM record the snapshot in progress state in info
                    }
                }

                val timeLimit = job.creation.timeLimit
                val startTime = metadata.creation.started.startTime
                timeLimit?.let {
                    if (timeLimitExceed(startTime, timeLimit))
                        return ExecutionResult.TimeLimitExceed(WorkflowType.CREATION)
                }
            }
            metadata.deletion.started != null -> {
                val snapshots = try {
                    client.getSnapshots(
                        "${smJobIdToPolicyName(job.id)}*",
                        job.snapshotConfig["repository"] as String
                    )
                } catch (ex: Exception) {
                    // TODO SM need to implement retry mechanism so we don't stuck forever
                    return ExecutionResult.Failure(ex, WorkflowType.DELETION, reset = false)
                }
                val existingSnapshots = snapshots.map { it.snapshotId().name }
                val startedDeleteSnapshots = metadata.deletion.started
                val remainingSnapshotsName = startedDeleteSnapshots.map { it.name }.toSet() - existingSnapshots.toSet()

                deletionStarted = if (remainingSnapshotsName.isEmpty()) {
                    null
                } else {
                    startedDeleteSnapshots.filter {
                        it.name in remainingSnapshotsName
                    }.toList()
                }

                val timeLimit = job.deletion.timeLimit
                val startTime = metadata.deletion.startedTime
                startTime?.let {
                    timeLimit?.let {
                        if (timeLimitExceed(startTime, timeLimit))
                            return ExecutionResult.TimeLimitExceed(WorkflowType.DELETION)
                    }
                }
            }
            else -> {
                log.info("No ongoing creating or deleting snapshots, will go to next execution schedule.")
            }
        }

        val metadataToSave = SMMetadata.Builder(metadata)
            .startedCreation(creationStarted)
            .startedDeletion(deletionStarted)
            .info(info)
            .build()

        if (creationStarted != null || deletionStarted != null) {
            return ExecutionResult.Stay(metadataToSave)
        }
        return ExecutionResult.Next(metadataToSave)
    }

    private fun timeLimitExceed(startTime: Instant, timeLimit: TimeValue): Boolean {
        return (now().toEpochMilli() - startTime.toEpochMilli()) > timeLimit.millis
    }
}
