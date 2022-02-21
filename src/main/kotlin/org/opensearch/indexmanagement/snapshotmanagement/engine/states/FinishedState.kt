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
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.upsert

// check the status of creating, deleting snapshot
object FinishedState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): Boolean {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        when {
            metadata.creating != null -> {
                val req = SnapshotsStatusRequest()
                    .snapshots(arrayOf("${metadata.creating}"))
                    .repository(job.snapshotConfig["repository"] as String)
                val res: SnapshotsStatusResponse = client.admin().cluster().suspendUntil { snapshotsStatus(req, it) }
                log.info("Get snapshot status: ${res.snapshots}")
                return if (res.snapshots.firstOrNull()?.state == SnapshotsInProgress.State.SUCCESS) {
                    context.metadataToSave = metadata.copy(
                        currentState = SMState.FINISHED.toString(),
                        creating = null,
                        info = metadata.info.upsert(
                            "last_success", "${metadata.creating} has been created."
                        )
                    )
                    true
                } else {
                    // We can record the snapshot in progress state in info
                    log.info("Creating snapshot [${metadata.creating}] has not succeed")
                    false

                    // TODO if timeout pass
                }
            }
            metadata.deleting != null -> {
                val req = SnapshotsStatusRequest()
                    .snapshots(arrayOf("${job.policyName}*"))
                    .repository(job.snapshotConfig["repository"] as String)
                val res: SnapshotsStatusResponse = context.client.admin().cluster().suspendUntil { snapshotsStatus(req, it) }
                log.info("Get snapshot status: ${res.snapshots}")
                val existingSnapshots = res.snapshots.map { it.snapshot.snapshotId.name }

                val remainingSnapshots = metadata.deleting.toSet() - existingSnapshots.toSet()
                return if (remainingSnapshots.isEmpty()) {
                    context.metadataToSave = metadata.copy(
                        currentState = SMState.FINISHED.toString(),
                        deleting = null
                    )
                    true
                } else {
                    context.metadataToSave = metadata.copy(
                        deleting = remainingSnapshots.toList()
                    )
                    false
                    // TODO if timeout pass
                }
            }
            else -> {
                log.info("Both creating and deleting are null.")
                return false
            }
        }
    }
}
