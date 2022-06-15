/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation

import org.apache.logging.log4j.Logger
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementException
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.generateSnapshotName
import org.opensearch.indexmanagement.snapshotmanagement.addSMPolicyInSnapshotMetadata
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshotsWithErrorHandling
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

object CreatingState : State {

    override val continuous: Boolean = false

    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.CREATION)

        var snapshotName: String? = metadata.creation.started?.first()

        // Check if there's already a snapshot created by SM in current execution period
        if (snapshotName == null) {
            val getSnapshotsResult = client.getSnapshotsWithErrorHandling(
                job, smDocIdToPolicyName(job.id) + "*", metadataBuilder,
                log, null, getSnapshotsErrorMessage(),
            )
            metadataBuilder = getSnapshotsResult.metadataBuilder
            if (getSnapshotsResult.failed) {
                return SMResult.Fail(metadataBuilder, WorkflowType.CREATION)
            }
            metadataBuilder.resetRetry(creation = true)
            val getSnapshots = getSnapshotsResult.snapshots

            val lastExecutionTime = job.creation.schedule.getPeriodStartingAt(null).v1()
            snapshotName = checkCreatedSnapshots(lastExecutionTime, getSnapshots)
            if (snapshotName != null) {
                log.info("sm dev already created snapshot $snapshotName")
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.IN_PROGRESS
                ).setCreationStarted(snapshotName)
                return SMResult.Next(metadataBuilder)
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
            metadataBuilder.setLatestExecution(
                status = SMMetadata.LatestExecution.Status.IN_PROGRESS,
            ).setCreationStarted(snapshotName)
        } catch (ex: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(ex) as Exception
            return handleException(unwrappedException, snapshotName, metadataBuilder, log)
        } catch (ex: Exception) {
            return handleException(ex, snapshotName, metadataBuilder, log)
        }
        metadataBuilder.resetRetry(creation = true)

        return SMResult.Next(metadataBuilder)
    }

    private fun handleException(ex: Exception, snapshotName: String, metadataBuilder: SMMetadata.Builder, log: Logger): SMResult {
        log.error(getCreateSnapshotErrorMessage(snapshotName), ex)
        metadataBuilder.setLatestExecution(
            status = SMMetadata.LatestExecution.Status.RETRYING,
            message = getCreateSnapshotErrorMessage(snapshotName),
            cause = SnapshotManagementException.wrap(ex).message,
        )
        return SMResult.Fail(metadataBuilder, WorkflowType.CREATION)
    }

    private fun getSnapshotsErrorMessage() = "Caught exception while getting snapshots to decide if snapshot has been created in previous execution schedule."

    private fun getCreateSnapshotErrorMessage(snapshotName: String) =
        "Caught exception while creating snapshot $snapshotName."

    /**
     * If there is snapshot already created in this execution period,
     * continue to next state with this snapshot name.
     */
    private fun checkCreatedSnapshots(lastExecutionTime: Instant, snapshots: List<SnapshotInfo>): String? {
        if (snapshots.isEmpty()) return null
        for (i in snapshots.sortedBy { it.startTime() }.indices.reversed()) {
            // Loop from the latest snapshots
            return if (!Instant.ofEpochMilli(snapshots[i].startTime()).isBefore(lastExecutionTime)) {
                snapshots[i].snapshotId().name
            } else {
                null
            }
        }
        return null
    }
}
