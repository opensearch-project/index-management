/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.upsert
import org.opensearch.indexmanagement.snapshotmanagement.smJobIdToPolicyName
import org.opensearch.snapshots.SnapshotMissingException
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

        var creationStarted = metadata.creation.started
        var deletionStarted = metadata.deletion.started
        var startedDeletionTime = metadata.deletion.startedTime
        var info = metadata.info

        metadata.creation.started?.let { started ->
            val snapshots = try {
                client.getSnapshots(
                    started.name,
                    job.snapshotConfig["repository"] as String
                )
            } catch (ex: SnapshotMissingException) {
                // User may manually delete the creating snapshot
                return SMResult.Failure(ex, WorkflowType.CREATION)
            } catch (ex: Exception) {
                log.error("Caught exception while get snapshots for started creation.", ex)
                return SMResult.Retry(WorkflowType.CREATION)
            }

            when (snapshots.firstOrNull()?.state()) {
                SnapshotState.SUCCESS -> {
                    creationStarted = null
                    info = info.upsert(
                        "last_success" to "$started"
                    )
                    // TODO SM notification snapshot created
                }
                else -> {
                    // IN_PROGRESS, FAILED, PARTIAL, INCOMPATIBLE
                    log.info("Creating snapshot [$started] has not succeed")
                }
            }

            // TODO SM if now is after next creation time, we can update nextCreationTime and try notify user

            job.creation.timeLimit?.let {
                if (timeLimitExceed(metadata.creation.started.startTime, it))
                    return SMResult.TimeLimitExceed(WorkflowType.CREATION)
            }
        }

        metadata.deletion.started?.let { startedDeleteSnapshots ->
            val snapshots = try {
                client.getSnapshots(
                    "${smJobIdToPolicyName(job.id)}*",
                    job.snapshotConfig["repository"] as String
                )
            } catch (ex: Exception) {
                log.error("Caught exception while get snapshots for started deletion.", ex)
                return SMResult.Retry(WorkflowType.DELETION)
            }

            val existingSnapshots = snapshots.map { it.snapshotId().name }
            val remainingSnapshotsName = startedDeleteSnapshots.map { it.name }.toSet() - existingSnapshots.toSet()

            deletionStarted = if (remainingSnapshotsName.isEmpty()) {
                // TODO SM notification snapshot deleted
                startedDeletionTime = null
                null
            } else {
                startedDeleteSnapshots.filter {
                    it.name in remainingSnapshotsName
                }.toList()
            }

            // TODO SM if now is after next deletion time, we can update nextDeletionTime and try notify user

            metadata.deletion.startedTime?.let { startTime ->
                job.deletion.timeLimit?.let {
                    if (timeLimitExceed(startTime, it))
                        return SMResult.TimeLimitExceed(WorkflowType.DELETION)
                }
            }
        }

        val metadataToSave = SMMetadata.Builder(metadata)
            .creation(creationStarted)
            .deletion(startedDeletionTime, deletionStarted)
            .info(info)
            .build()

        if (creationStarted != null || deletionStarted != null) {
            return SMResult.Stay(metadataToSave)
        }
        return SMResult.Next(metadataToSave)
    }

    private fun timeLimitExceed(startTime: Instant, timeLimit: TimeValue): Boolean {
        return (now().toEpochMilli() - startTime.toEpochMilli()) > timeLimit.millis
    }
}
