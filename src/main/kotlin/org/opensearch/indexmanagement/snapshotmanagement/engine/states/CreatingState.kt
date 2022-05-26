/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.generateSnapshotName
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.snapshots.SnapshotMissingException
import java.time.Instant
import java.time.Instant.now

object CreatingState : State {

    override val continuous: Boolean = false

    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var snapshotName: String?

        val lastExecutionTime = job.creation.schedule.getPeriodStartingAt(null).v1()
        val getSnapshots = try {
            client.getSnapshots(
                smDocIdToPolicyName(job.id) + "*",
                job.snapshotConfig["repository"] as String
            ).sortedBy { it.startTime() }
        } catch (ex: SnapshotMissingException) {
            emptyList()
        } catch (ex: Exception) {
            log.error("Caught exception while getting snapshots to decide if snapshot has been created in previous execution schedule.", ex)
            return SMResult.Retry(WorkflowType.CREATION)
        }

        snapshotName = checkCreatedSnapshots(lastExecutionTime, getSnapshots)
        if (snapshotName == null) {
            snapshotName = generateSnapshotName(job)
            log.info("sm dev: Snapshot to create: $snapshotName.")
            try {
                val req = CreateSnapshotRequest(job.snapshotConfig["repository"] as String, snapshotName)
                    .source(job.snapshotConfig)
                    .waitForCompletion(false)
                val res: CreateSnapshotResponse = client.admin().cluster().suspendUntil { createSnapshot(req, it) }
                // TODO SM notification that snapshot starts to be created
                log.info("sm dev: Create snapshot response: $res.")
            } catch (ex: Exception) {
                return SMResult.Failure(ex, WorkflowType.CREATION, notifiable = true)
            }
        }

        val metadataToSave = SMMetadata.Builder(metadata)
            .creation(
                SMMetadata.SnapshotInfo(
                    name = snapshotName,
                    startTime = now(),
                )
            )
            .resetRetry(creation = true)
            .build()
        return SMResult.Next(metadataToSave)
    }

    /**
     * If there is snapshot created after last execution time,
     * continue to next state with this snapshot name.
     */
    private fun checkCreatedSnapshots(lastExecutionTime: Instant, snapshots: List<SnapshotInfo>): String? {
        if (snapshots.isEmpty()) return null
        for (i in snapshots.indices.reversed()) {
            return if (!Instant.ofEpochMilli(snapshots[i].startTime()).isBefore(lastExecutionTime)) {
                snapshots[i].snapshotId().name
            } else {
                null
            }
        }
        return null
    }
}
