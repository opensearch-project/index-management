/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.apache.logging.log4j.LogManager
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import java.time.Instant

// check the status of creating, deleting snapshot
object DeleteConditionMetState : State {

    private val log = LogManager.getLogger(javaClass)

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): ExecutionResult {
        val job = context.job
        val metadata = context.metadata

        if (metadata.deletion.started != null) {
            log.info("Snapshots deleting by snapshot management: [${metadata.deletion.started}].")
            return ExecutionResult.Stay()
        }

        val nextDeletionTime = metadata.deletion.trigger.time
        val nextDeletionTimeToSave: Instant
        if (!Instant.now().isBefore(nextDeletionTime)) {
            log.info("sm dev current time [${Instant.now()}] has passed nextDeletionTime [$nextDeletionTime]")
            nextDeletionTimeToSave = getNextExecutionTime(job.deletion.schedule, Instant.now())
        } else {
            log.info("sm dev: current time [${Instant.now()}] has not passed nextDeletionTime [$nextDeletionTime]")
            // TODO SM dynamically update job start_time to avoid unnecessary job runs
            return ExecutionResult.Stay()
        }

        val metadataToSave = SMMetadata.Builder(metadata)
            .currentState(SMState.DELETE_CONDITION_MET)
            .nextDeletionTime(nextDeletionTimeToSave)
            .build()
        log.info("sm dev: Save current state as DELETE_CONDITION_MET [$metadataToSave]")
        return ExecutionResult.Next(metadataToSave)
    }
}
