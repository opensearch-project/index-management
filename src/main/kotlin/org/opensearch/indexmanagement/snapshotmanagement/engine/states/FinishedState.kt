/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse
import org.opensearch.cluster.SnapshotsInProgress
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.upsert

object FinishedState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): ExecutionResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        when {
            metadata.creation.started != null -> {
                val req = SnapshotsStatusRequest()
                    .snapshots(arrayOf("${metadata.creation.started}"))
                    .repository(job.snapshotConfig["repository"] as String)
                val res: SnapshotsStatusResponse = client.admin().cluster().suspendUntil { snapshotsStatus(req, it) }
                log.info("Get snapshot status: ${res.snapshots}")
                return if (res.snapshots.firstOrNull()?.state == SnapshotsInProgress.State.SUCCESS) {
                    val metadataToSave = metadata.copy(
                        currentState = SMState.FINISHED,
                        creation = metadata.creation.copy(started = null),
                        info = metadata.info.upsert(
                            "last_success" to "${metadata.creation.started} has been created."
                        )
                    )
                    ExecutionResult.Next(metadataToSave)
                } else {
                    // We can record the snapshot in progress state in info
                    log.info("Creating snapshot [${metadata.creation.started}] has not succeed")
                    ExecutionResult.NotMet(false)

                    // TODO if timeout pass
                }
            }
            metadata.deletion.started != null -> {
                val req = SnapshotsStatusRequest()
                    .snapshots(arrayOf("${job.policyName}*"))
                    .repository(job.snapshotConfig["repository"] as String)
                val res: SnapshotsStatusResponse = context.client.admin().cluster().suspendUntil { snapshotsStatus(req, it) }
                log.info("Get snapshot status: ${res.snapshots}")
                val existingSnapshots = res.snapshots.map { it.snapshot.snapshotId.name }

                val startedDeleteSnapshots = metadata.deletion.started
                val remainingSnapshotsName = startedDeleteSnapshots.map { it.name }.toSet() - existingSnapshots.toSet()
                return if (remainingSnapshotsName.isEmpty()) {
                    val metadataToSave = metadata.copy(
                        currentState = SMState.FINISHED,
                        deletion = metadata.deletion.copy(started = null),
                    )
                    ExecutionResult.Next(metadataToSave)
                } else {
                    val remainingSnapshots = startedDeleteSnapshots.filter {
                        it.name in remainingSnapshotsName
                    }
                    val metadataToSave = metadata.copy(
                        deletion = metadata.deletion.copy(
                            started = remainingSnapshots.toList()
                        ),
                    )
                    ExecutionResult.NotMet(false, metadataToSave)
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
    }
}
