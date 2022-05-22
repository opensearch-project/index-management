/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.Result
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

    override suspend fun execute(context: SMStateMachine): Result {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var creationStarted = metadata.creation.started
        var deletionStarted = metadata.deletion.started
        var info = metadata.info

        metadata.creation.started?.let { started ->
            val snapshots = try {
                client.getSnapshots(
                    started.name,
                    job.snapshotConfig["repository"] as String
                )
            } catch (ex: SnapshotMissingException) {
                // User may manually delete the creating snapshot
                return Result.Failure(ex, WorkflowType.CREATION, reset = true)
            } catch (ex: Exception) {
                // TODO SM need to implement retry mechanism so we don't stuck forever
                return Result.Failure(ex, WorkflowType.CREATION, reset = false)
            }

            when (snapshots.firstOrNull()?.state()) {
                SnapshotState.SUCCESS -> {
                    creationStarted = null
                    info = info.upsert(
                        "last_success" to "$started"
                    )
                }
                else -> {
                    // IN_PROGRESS, FAILED, PARTIAL, INCOMPATIBLE
                    log.info("Creating snapshot [$started] has not succeed")
                    // TODO SM record the snapshot in progress state in info
                }
            }

            val timeLimit = job.creation.timeLimit
            val startTime = metadata.creation.started.startTime
            timeLimit?.let {
                if (timeLimitExceed(startTime, timeLimit))
                    return Result.TimeLimitExceed(WorkflowType.CREATION)
            }
        }

        metadata.deletion.started?.let { startedDeleteSnapshots ->
            val snapshots = try {
                client.getSnapshots(
                    "${smJobIdToPolicyName(job.id)}*",
                    job.snapshotConfig["repository"] as String
                )
            } catch (ex: Exception) {
                // TODO SM need to implement retry mechanism so we don't stuck forever
                return Result.Failure(ex, WorkflowType.DELETION, reset = false)
            }
            val existingSnapshots = snapshots.map { it.snapshotId().name }
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
                        return Result.TimeLimitExceed(WorkflowType.DELETION)
                }
            }
        }

        val metadataToSave = SMMetadata.Builder(metadata)
            .startedCreation(creationStarted)
            .startedDeletion(deletionStarted)
            .info(info)
            .build()

        if (creationStarted != null || deletionStarted != null) {
            return Result.Stay(metadataToSave)
        }
        return Result.Next(metadataToSave)
    }

    private fun timeLimitExceed(startTime: Instant, timeLimit: TimeValue): Boolean {
        return (now().toEpochMilli() - startTime.toEpochMilli()) > timeLimit.millis
    }
}
