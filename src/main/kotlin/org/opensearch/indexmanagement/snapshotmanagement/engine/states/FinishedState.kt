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
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
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
                log.warn("Snapshot ${started.name} not found while checking if it has been created.")
                return SMResult.Failure(ex, WorkflowType.CREATION)
            } catch (ex: Exception) {
                log.error("Caught exception while getting started creation snapshot [${started.name}].", ex)
                return SMResult.Retry(WorkflowType.CREATION)
            }

            val snapshot = snapshots.firstOrNull() ?: return SMResult.Retry(WorkflowType.CREATION)
            when (snapshot.state()) {
                SnapshotState.IN_PROGRESS -> {
                    job.creation.timeLimit?.let {
                        if (timeLimitExceed(metadata.creation.started.startTime, it))
                            return SMResult.TimeLimitExceed(WorkflowType.CREATION)
                    }
                }
                SnapshotState.SUCCESS -> {
                    creationStarted = null
                    info = info.upsert(
                        "last_success" to started.name
                    )
                    // TODO SM notification snapshot created
                }
                else -> {
                    // FAILED, PARTIAL, INCOMPATIBLE
                    creationStarted = null
                    // TODO SM notification snapshot creation has problem
                }
            }

            // TODO SM if now is after next creation time, update nextCreationTime to next execution schedule
            //  and try notify user that we skip the execution because snapshot creation time
            //  is longer than execution schedule
        }

        metadata.deletion.started?.let { startedDeleteSnapshots ->
            val snapshots = try {
                client.getSnapshots(
                    "${smDocIdToPolicyName(job.id)}*",
                    job.snapshotConfig["repository"] as String
                )
            } catch (ex: SnapshotMissingException) {
                log.warn("No snapshots found under policy while getting snapshots to decide if snapshots has been deleted.")
                return SMResult.Failure(ex, WorkflowType.DELETION)
            } catch (ex: Exception) {
                log.error("Caught exception while getting snapshots to decide if snapshots [$startedDeleteSnapshots] has been deleted.", ex)
                return SMResult.Retry(WorkflowType.DELETION)
            }

            val existingSnapshotsNameSet = snapshots.map { it.snapshotId().name }.toSet()
            val startedDeletionSnapshotsNameSet = startedDeleteSnapshots.map { it.name }.toSet()
            val remainingSnapshotsName = existingSnapshotsNameSet intersect startedDeletionSnapshotsNameSet
            deletionStarted = if (remainingSnapshotsName.isEmpty()) {
                startedDeletionTime = null
                // TODO SM notification snapshot deleted
                null
            } else {
                metadata.deletion.startedTime?.let { startTime ->
                    job.deletion.timeLimit?.let {
                        if (timeLimitExceed(startTime, it))
                            return SMResult.TimeLimitExceed(WorkflowType.DELETION)
                    }
                }

                startedDeleteSnapshots.filter {
                    it.name in remainingSnapshotsName
                }.toList()
            }

            // TODO SM if now is after next deletion time, we can update nextDeletionTime and try notify user
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
