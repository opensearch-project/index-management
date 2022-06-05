/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementException
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.generateSnapshotName
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.addSMPolicyInSnapshotMetadata
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshotsWithErrorHandling
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.snapshots.SnapshotInfo
import java.time.Instant
import java.time.Instant.now

object CreatingState : State {

    override val continuous: Boolean = false

    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .setWorkflow(WorkflowType.CREATION)

        var snapshotName: String? = metadata.creation.started?.first()

        if (snapshotName !== null) {
            metadataBuilder.creation(
                snapshot = snapshotName,
                initLatestExecution = SMMetadata.LatestExecution.init(
                    status = SMMetadata.LatestExecution.Status.IN_PROGRESS,
                )
            )
            return SMResult.Next(metadataBuilder.build())
        } else {
            val getSnapshotsRes = client.getSnapshotsWithErrorHandling(
                job,
                smDocIdToPolicyName(job.id) + "*",
                metadataBuilder,
                log,
                null,
                GET_SNAPSHOTS_ERROR_MESSAGE,
            )
            metadataBuilder = getSnapshotsRes.metadataBuilder
            if (getSnapshotsRes.failed)
                return SMResult.Failure(metadataBuilder.build(), WorkflowType.CREATION)
            metadataBuilder.resetRetry(creation = true)
            val getSnapshots = getSnapshotsRes.snapshots
            log.info("sm dev getSnapshots ${getSnapshots.map { it.snapshotId().name }}")

            val lastExecutionTime = job.creation.schedule.getPeriodStartingAt(null).v1()
            log.info("sm dev last execution time $lastExecutionTime")
            snapshotName = checkCreatedSnapshots(lastExecutionTime, getSnapshots)
            if (snapshotName != null) {
                metadataBuilder.creation(
                    snapshot = snapshotName,
                    initLatestExecution = SMMetadata.LatestExecution.init(
                        status = SMMetadata.LatestExecution.Status.IN_PROGRESS,
                    )
                )
                return SMResult.Next(metadataBuilder.build())
            }
        }

        snapshotName = generateSnapshotName(job)
        log.info("sm dev: Snapshot to create: $snapshotName.")
        try {
            val req = CreateSnapshotRequest(job.snapshotConfig["repository"] as String, snapshotName)
                .source(addSMPolicyInSnapshotMetadata(job.snapshotConfig, job.policyName))
                .waitForCompletion(false)
            val res: CreateSnapshotResponse = client.admin().cluster().suspendUntil { createSnapshot(req, it) }
            // TODO SM notification that snapshot starts to be created
            log.info("sm dev: Create snapshot response: $res.")
            metadataBuilder.creation(
                snapshot = snapshotName,
                initLatestExecution = SMMetadata.LatestExecution(
                    status = SMMetadata.LatestExecution.Status.IN_PROGRESS,
                    startTime = now(),
                )
            )
        } catch (ex: Exception) {
            log.error(getCreateSnapshotErrorMessage(snapshotName), ex)
            metadataBuilder.creation(
                snapshot = snapshotName,
                initLatestExecution = SMMetadata.LatestExecution.init(
                    status = SMMetadata.LatestExecution.Status.RETRYING,
                    info = SMMetadata.Info(
                        message = getCreateSnapshotErrorMessage(snapshotName),
                        cause = SnapshotManagementException.wrap(ex).message
                    )
                )
            )

            return SMResult.Failure(metadataBuilder.build(), WorkflowType.CREATION)
        }
        metadataBuilder.resetRetry(creation = true)

        return SMResult.Next(metadataBuilder.build())
    }

    private const val GET_SNAPSHOTS_ERROR_MESSAGE = "Caught exception while getting snapshots to decide if snapshot has been created in previous execution schedule."
    private fun getCreateSnapshotErrorMessage(snapshotName: String) = "Caught exception while creating snapshot $snapshotName."

    /**
     * If there is snapshot already created in this execution period,
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
