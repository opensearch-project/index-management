/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SnapshotManagementException
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SnapshotManagementException.ExceptionCode.API_CALLING
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.generateSnapshotName
import org.opensearch.indexmanagement.snapshotmanagement.startTransaction

object CreatingState : State {

    override val continuous: Boolean = false

    override suspend fun execute(context: SMStateMachine): ExecutionResult {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        if (metadata.apiCalling) {
            return ExecutionResult.Failure(SnapshotManagementException(API_CALLING))
        }

        context.startTransaction()

        val snapshotName = generateSnapshotName(job)
        log.info("Snapshot name to create is $snapshotName")

        val res: CreateSnapshotResponse
        try {
            val req = CreateSnapshotRequest(job.snapshotConfig["repository"] as String, snapshotName)
                .source(job.snapshotConfig)
                .waitForCompletion(false)
            res = client.admin().cluster().suspendUntil { createSnapshot(req, it) }
        } catch (ex: Exception) {
            return ExecutionResult.Failure(ex)
        }

        log.info("Create snapshot response status code: ${res.status()}")
        context.metadataToSave = metadata.copy(
            currentState = SMState.CREATING,
            apiCalling = false,
            creation = metadata.creation.copy(
                started = snapshotName
            ),
        )
        return ExecutionResult.Next
    }
}
