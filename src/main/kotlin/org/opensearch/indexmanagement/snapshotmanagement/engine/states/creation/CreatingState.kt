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
import org.opensearch.indexmanagement.snapshotmanagement.addSMPolicyInSnapshotMetadata
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.generateSnapshotName
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.snapshots.ConcurrentSnapshotExecutionException
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.ClusterAdminClient
import java.time.Instant

object CreatingState : State {
    override val continuous: Boolean = false

    @Suppress("ReturnCount")
    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder =
            SMMetadata.Builder(metadata)
                .workflow(WorkflowType.CREATION)

        if (job.creation == null) {
            log.warn("Policy creation config becomes null before trying to create snapshot. Reset.")
            return SMResult.Fail(
                metadataBuilder.resetCreation(), WorkflowType.CREATION, true,
            )
        }

        var snapshotName: String? = metadata.creation?.started?.first()

        // Check if there's already a snapshot created by SM in current execution period.
        // So that this State can be executed idempotent.
        if (snapshotName == null) {
            val getSnapshotsResult =
                client.getSnapshots(
                    job, job.policyName + "*", metadataBuilder,
                    log, null, SNAPSHOT_ERROR_MESSAGE, true,
                )
            metadataBuilder = getSnapshotsResult.metadataBuilder
            if (getSnapshotsResult.failed) {
                return SMResult.Fail(metadataBuilder, WorkflowType.CREATION)
            }
            val getSnapshots = getSnapshotsResult.snapshots

            val latestExecutionStartTime = job.creation?.schedule?.getPeriodStartingAt(null)?.v1()
            snapshotName = latestExecutionStartTime?.let { checkCreatedSnapshots(it, getSnapshots) }
            if (snapshotName != null) {
                log.info("Already created snapshot [$snapshotName] during this execution period starting at $latestExecutionStartTime.")
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.IN_PROGRESS,
                ).setCreationStarted(snapshotName)
                return SMResult.Next(metadataBuilder)
            }
        }

        snapshotName = generateSnapshotName(job)
        try {
            val req =
                CreateSnapshotRequest(job.snapshotConfig["repository"] as String, snapshotName)
                    .source(addSMPolicyInSnapshotMetadata(job.snapshotConfig, job.policyName))
                    .waitForCompletion(false)
            client.admin().cluster().suspendUntil<ClusterAdminClient, CreateSnapshotResponse> { createSnapshot(req, it) }

            metadataBuilder.setLatestExecution(
                status = SMMetadata.LatestExecution.Status.IN_PROGRESS,
                message = getSnapshotCreationStartedMessage(snapshotName),
            ).setCreationStarted(snapshotName)
        } catch (ex: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(ex) as Exception
            return handleException(unwrappedException, snapshotName, metadataBuilder, log)
        } catch (ex: Exception) {
            return handleException(ex, snapshotName, metadataBuilder, log)
        }

        return SMResult.Next(metadataBuilder)
    }

    private fun handleException(ex: Exception, snapshotName: String, metadataBuilder: SMMetadata.Builder, log: Logger): SMResult {
        if (ex is ConcurrentSnapshotExecutionException) {
            log.error(CONCURRENT_SNAPSHOT_MESSAGE, ex)
            metadataBuilder.setLatestExecution(
                status = SMMetadata.LatestExecution.Status.RETRYING,
                message = CONCURRENT_SNAPSHOT_MESSAGE,
            )
            return SMResult.Stay(metadataBuilder)
        }
        log.error(getCreateSnapshotErrorMessage(snapshotName), ex)
        metadataBuilder.setLatestExecution(
            status = SMMetadata.LatestExecution.Status.RETRYING,
            message = getCreateSnapshotErrorMessage(snapshotName),
            cause = ex,
        )
        return SMResult.Fail(metadataBuilder, WorkflowType.CREATION)
    }

    const val CONCURRENT_SNAPSHOT_MESSAGE = "Concurrent snapshot exception happened, retrying..."

    private fun getSnapshotCreationStartedMessage(snapshotName: String) =
        "Snapshot $snapshotName creation has been started and waiting for completion."

    private const val SNAPSHOT_ERROR_MESSAGE =
        "Caught exception while getting snapshots to decide if snapshot has been created in previous execution period."

    private fun getCreateSnapshotErrorMessage(snapshotName: String) =
        "Caught exception while creating snapshot $snapshotName."

    /**
     * If there is snapshot already created in this execution period,
     * continue to next state with this snapshot name.
     */
    @Suppress("ReturnCount")
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
