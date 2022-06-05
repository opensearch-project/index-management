/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementException
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.filterBySMPolicyInSnapshotMetadata
import org.opensearch.indexmanagement.snapshotmanagement.getSnapshots
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.preFixTimeStamp
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.snapshots.SnapshotMissingException
import org.opensearch.snapshots.SnapshotState
import java.time.Instant
import java.time.Instant.now

object FinishedState : State {

    override val continuous = true

    private fun getSnapshotMissingMessageInCreationWorkflow(snapshot: String) = "Snapshot $snapshot not found while checking if it has been created."
    private fun getSnapshotExceptionInCreationWorkflow(snapshot: String) = "Caught exception while getting started creation snapshot [$snapshot]."
    private fun getSnapshotMissingMessageInDeletionWorkflow() = "No snapshots found under policy while getting snapshots to decide if snapshots has been deleted."
    private fun getSnapshotExceptionInDeletionWorkflow(startedDeleteSnapshots: List<String>) = "Caught exception while getting snapshots to decide if snapshots [$startedDeleteSnapshots] has been deleted."
    override suspend fun execute(context: SMStateMachine): SMResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        val metadataBuilder = SMMetadata.Builder(metadata)

        metadata.creation.started?.first()?.let { started ->
            assert(metadata.creation.latestExecution != null)
            metadataBuilder.setWorkflow(WorkflowType.CREATION)

            val snapshots = try {
                client.getSnapshots(
                    started,
                    job.snapshotConfig["repository"] as String
                )
            } catch (ex: SnapshotMissingException) {
                log.warn(getSnapshotMissingMessageInCreationWorkflow(started))
                emptyList()
            } catch (ex: Exception) {
                log.error(getSnapshotExceptionInCreationWorkflow(started), ex)
                metadataBuilder.updateLatestExecution(
                    status = SMMetadata.LatestExecution.Status.RETRYING,
                    message = getSnapshotExceptionInCreationWorkflow(started),
                    cause = SnapshotManagementException.wrap(ex).message
                )
                return SMResult.Failure(metadataBuilder.build(), WorkflowType.CREATION)
            }.filterBySMPolicyInSnapshotMetadata(job.policyName)
            metadataBuilder.resetRetry(creation = true)

            if (snapshots.isEmpty()) {
                metadataBuilder
                    .updateLatestExecution(
                        SMMetadata.LatestExecution.Status.FAILED,
                        message = getSnapshotMissingMessageInCreationWorkflow(started),
                        endTime = now(),
                    )
                    .reset()
            }
            val snapshot = snapshots.first()
            when (snapshot.state()) {
                SnapshotState.SUCCESS -> {
                    metadataBuilder
                        .creation(snapshot = null)
                        .updateLatestExecution(
                            status = SMMetadata.LatestExecution.Status.SUCCESS,
                            message = "Snapshot ${metadata.creation.started.first()} creation end with state ${snapshot.state()}.",
                            endTime = now(),
                        )
                    // TODO SM notification snapshot created
                }
                SnapshotState.IN_PROGRESS -> {
                    job.creation.timeLimit?.let {
                        if (timeLimitExceed(metadata.creation.latestExecution!!.startTime, it))
                            return SMResult.TimeLimitExceed(metadataBuilder.build(), WorkflowType.CREATION)
                    }
                }
                else -> {
                    // FAILED, PARTIAL, INCOMPATIBLE
                    metadataBuilder
                        .creation(null)
                        .updateLatestExecution(
                            status = SMMetadata.LatestExecution.Status.FAILED,
                            cause = "Snapshot ${metadata.creation.started.first()} creation end with state ${snapshot.state()}.",
                            endTime = now(),
                        )
                    // TODO SM notification snapshot creation has problem
                }
            }

            // TODO SM if now is after next creation time, update nextCreationTime to next execution schedule
            //  and try notify user that we skip the execution because snapshot creation time
            //  is longer than execution schedule
        }

        metadata.deletion.started?.let { startedDeleteSnapshots ->
            assert(metadata.deletion.latestExecution != null)
            metadataBuilder.setWorkflow(WorkflowType.DELETION)
            val snapshots = try {
                client.getSnapshots(
                    "${smDocIdToPolicyName(job.id)}*",
                    job.snapshotConfig["repository"] as String
                )
            } catch (ex: SnapshotMissingException) {
                log.warn(getSnapshotMissingMessageInDeletionWorkflow())
                emptyList()
            } catch (ex: Exception) {
                log.error(getSnapshotExceptionInDeletionWorkflow(startedDeleteSnapshots), ex)
                metadataBuilder.updateLatestExecution(
                    status = SMMetadata.LatestExecution.Status.RETRYING,
                    message = getSnapshotExceptionInDeletionWorkflow(startedDeleteSnapshots),
                    cause = SnapshotManagementException.wrap(ex).message
                )
                return SMResult.Failure(metadataBuilder.build(), WorkflowType.DELETION)
            }.filterBySMPolicyInSnapshotMetadata(job.policyName)
            metadataBuilder.resetRetry(deletion = true)

            if (snapshots.isEmpty()) {
                metadataBuilder.updateLatestExecution(
                        SMMetadata.LatestExecution.Status.FAILED,
                        message = getSnapshotMissingMessageInDeletionWorkflow(),
                        endTime = now(),
                    )
                    .reset()
            }

            val existingSnapshotsNameSet = snapshots.map { it.snapshotId().name }.toSet()
            val remainingSnapshotsName = existingSnapshotsNameSet intersect startedDeleteSnapshots.toSet()
            if (remainingSnapshotsName.isEmpty()) {
                log.info("Snapshots have been deleted: $existingSnapshotsNameSet.")
                // TODO SM notification snapshot deleted
                metadataBuilder.deletion(null)
                    .updateLatestExecution(
                        status = SMMetadata.LatestExecution.Status.SUCCESS,
                        message = "Snapshots ${metadata.creation.started} deletion has finished.",
                        endTime = now(),
                    )
            } else {
                job.deletion.timeLimit?.let {
                    if (timeLimitExceed(metadata.deletion.latestExecution!!.startTime, it))
                        return SMResult.TimeLimitExceed(metadataBuilder.build(), WorkflowType.DELETION)
                }

                log.info("Snapshots haven't been deleted: $remainingSnapshotsName.")
                metadataBuilder.deletion(
                    remainingSnapshotsName.toList(),
                )
            }

            // TODO SM if now is after next deletion time, we can update nextDeletionTime and try notify user
        }

        val metadataToSave = metadataBuilder.build()
        if (metadataToSave.creation.started != null || metadataToSave.deletion.started != null) {
            return SMResult.Stay(metadataToSave)
        }
        return SMResult.Next(metadataToSave)
    }

    private fun timeLimitExceed(startTime: Instant, timeLimit: TimeValue): Boolean {
        return (now().toEpochMilli() - startTime.toEpochMilli()) > timeLimit.millis
    }
}
