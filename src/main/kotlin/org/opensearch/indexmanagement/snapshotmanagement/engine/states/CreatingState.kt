/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.generateSnapshotName
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.ResetType

object CreatingState : State {

    override val continuous: Boolean = false

    override suspend fun execute(context: SMStateMachine): ExecutionResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var snapshotName: String
        // TODO SM Check if there is any snapshot exists between last execution time and now()
        val lastExecutionTime = job.creation.schedule.getPeriodStartingAt(null).v1()


        snapshotName = generateSnapshotName(job)
        log.info("Snapshot to create: $snapshotName.")

        val res: CreateSnapshotResponse
        try {
            val req = CreateSnapshotRequest(job.snapshotConfig["repository"] as String, snapshotName)
                .source(job.snapshotConfig)
                .waitForCompletion(false)
            res = client.admin().cluster().suspendUntil { createSnapshot(req, it) }
        }
        // catch (ex: RepositoryMissingException) {
        //     return ExecutionResult.Failure(SnapshotManagementException(ex), ActionType.CREATION)
        // }
        catch (ex: Exception) {
            return ExecutionResult.Failure(ex, ResetType.CREATION)
        }

        log.info("Create snapshot response: $res.")

        val metadataToSave = SMMetadata.Builder(metadata)
            .currentState(SMState.CREATING)
            .startedCreation(SMMetadata.SnapshotInfo(name = snapshotName))
            .build()
        return ExecutionResult.Next(metadataToSave)
    }
}
