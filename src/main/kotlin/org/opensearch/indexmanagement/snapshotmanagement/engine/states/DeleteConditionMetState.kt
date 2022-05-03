/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.apache.logging.log4j.LogManager
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import java.time.Instant

// check the status of creating, deleting snapshot
object DeleteConditionMetState : State {

    private val log = LogManager.getLogger(javaClass)

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): ExecutionResult {
        val job = context.job
        val metadata = context.metadata

        if (metadata.deletion.started != null) {
            log.info("There is already snapshot being deleted: ${metadata.deletion.started}.")
            return ExecutionResult.NotMet()
        }

        val nextDeletionTime = metadata.deletion.trigger.time
        val nextDeletionTimeToSave: Instant
        if (!Instant.now().isBefore(nextDeletionTime)) {
            log.info("current time [${Instant.now()}] has passed nextDeletionTime [$nextDeletionTime]")
            nextDeletionTimeToSave = getNextExecutionTime(job.deletion.schedule, Instant.now())
        } else {
            log.info("current time [${Instant.now()}] has not passed nextDeletionTime [$nextDeletionTime]")
            // TODO dynamically update job start_time
            return ExecutionResult.NotMet()
        }

        val metadataToSave = metadata.copy(
            currentState = SMState.CREATE_CONDITION_MET,
            deletion = metadata.deletion.copy(
                trigger = metadata.deletion.trigger.copy(
                    time = nextDeletionTimeToSave
                )
            ),
        )
        log.info("Save current state as DELETE_CONDITION_MET [$metadataToSave]")
        return ExecutionResult.Next(metadataToSave)
    }
}
