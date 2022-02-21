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
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.StateMachineExecutionException
import org.opensearch.indexmanagement.snapshotmanagement.generateFormatTime
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.upsert
import org.opensearch.indexmanagement.snapshotmanagement.revertTransaction
import org.opensearch.indexmanagement.snapshotmanagement.startTransaction
import org.opensearch.rest.RestStatus

object CreatingState : State {

    override val continuous: Boolean = false

    override suspend fun execute(context: SMStateMachine): Boolean {
        val client = context.client
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        if (metadata.apiCalling) {
            // probably because we cannot index metadata after transaction
            context.metadataToSave = metadata.copy(
                currentState = SMState.FINISHED.toString(),
                info = metadata.info.upsert(
                    "transaction_failure",
                    "Undetermined about whether the last snapshot has been created, will go to next schedule."
                )
            )
            return true
        }

        context.startTransaction()

        val snapshotName = "${job.policyName}-${generateFormatTime(job.snapshotConfig["date_format"] as String)}"
        log.info("Snapshot name to create is $snapshotName")

        val res: CreateSnapshotResponse
        try {
            val req = CreateSnapshotRequest(job.snapshotConfig["repository"] as String, snapshotName)
                .source(job.snapshotConfig)
                .waitForCompletion(false)
            res = client.admin().cluster().suspendUntil { createSnapshot(req, it) }
        } catch (ex: Exception) {
            // TODO catch RepositoryMissingException, shown to user in info
            context.revertTransaction()
            throw StateMachineExecutionException(cause = ex)
        }

        return if (RestStatus.ACCEPTED == res.status()) {
            log.info("Create snapshot response status code: ${res.status()}")
            context.metadataToSave = metadata.copy(
                currentState = SMState.CREATING.toString(),
                apiCalling = false,
                creating = snapshotName
            )
            true
        } else {
            // Based on status() in CreateSnapshotResponse
            // ACCEPTED is the only successful response status for our call
            throw StateMachineExecutionException("Create snapshot $snapshotName returns ${res.status()}, expecting ACCEPTED 202.")
        }
    }
}
