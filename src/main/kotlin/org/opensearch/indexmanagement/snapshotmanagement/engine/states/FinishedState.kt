/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.ResetType
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.upsert
import org.opensearch.indexmanagement.snapshotmanagement.smJobIdToPolicyName
import org.opensearch.snapshots.SnapshotState

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
                try {
                    val snapshots = client.getSnapshots(
                        metadata.creation.started.name,
                        job.snapshotConfig["repository"] as String
                    )
                    when (snapshots.firstOrNull()?.state()) {
                        SnapshotState.SUCCESS -> {
                            creationStarted = null
                            info = info.upsert(
                                "last_success" to "${metadata.creation.started} has been created."
                            )
                        }
                        else -> {
                            // We can record the snapshot in progress state in info
                            log.info("Creating snapshot [${metadata.creation.started}] has not succeed")

                            // TODO if timeout pass
                        }
                    }
                } catch (ex: Exception) {
                    // if someone deletes the creating snapshot, we could face SnapshotMissingException
                    return ExecutionResult.Failure(ex, ResetType.CREATION)
                }
            }
            metadata.deletion.started != null -> {
                val req = SnapshotsStatusRequest()
                    .snapshots(arrayOf("${smJobIdToPolicyName(job.id)}*"))
                    .repository(job.snapshotConfig["repository"] as String)
                val res: SnapshotsStatusResponse = context.client.admin().cluster().suspendUntil { snapshotsStatus(req, it) }
                log.info("Get snapshot status: ${res.snapshots}")
                val existingSnapshots = res.snapshots.map { it.snapshot.snapshotId.name }

                val startedDeleteSnapshots = metadata.deletion.started
                val remainingSnapshotsName = startedDeleteSnapshots.map { it.name }.toSet() - existingSnapshots.toSet()
                if (remainingSnapshotsName.isEmpty()) {
                    deletionStarted = null
                } else {
                    val remainingSnapshots = startedDeleteSnapshots.filter {
                        it.name in remainingSnapshotsName
                    }
                    deletionStarted = remainingSnapshots.toList()

                    // TODO if timeout pass
                }
            }
            else -> {
                // TODO not supposed to enter here
                log.info("Both creating and deleting are null.")
                val metadataToSave = metadata.copy(
                    currentState = SMState.FINISHED
                )
                return ExecutionResult.Next(metadataToSave)
            }
        }

        val metadataToSave: SMMetadata = metadata.copy(
            creation = metadata.creation.copy(
                started = creationStarted
            ),
            deletion = metadata.deletion.copy(
                started = deletionStarted
            ),
            info = info
        )
        if (creationStarted != null || deletionStarted != null) {
            return ExecutionResult.Stay(metadataToSave = metadataToSave)
        }

        return ExecutionResult.Next(
            metadataToSave.copy(
                currentState = SMState.FINISHED
            )
        )
    }
}
