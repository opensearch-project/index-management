/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import java.time.Instant
import java.time.Instant.now

object CreateConditionMetState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): ExecutionResult {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        if (metadata.creation.started != null) {
            log.info("There is an ongoing snapshot creation: ${metadata.creation.started}.")
            return ExecutionResult.Stay()
        }

        val nextCreationTime = metadata.creation.trigger.time
        val nextCreationTimeToSave: Instant
        if (!now().isBefore(nextCreationTime)) {
            log.info("current time [${now()}] has passed nextCreationTime [$nextCreationTime]")
            nextCreationTimeToSave = getNextExecutionTime(job.creation.schedule, now())
        } else {
            log.info("current time [${now()}] has not passed nextCreationTime [$nextCreationTime]")
            // TODO dynamically update job start_time
            return ExecutionResult.Stay()
        }

        val metadataToSave = metadata.copy(
            currentState = SMState.CREATE_CONDITION_MET,
            creation = metadata.creation.copy(
                trigger = metadata.creation.trigger.copy(
                    time = nextCreationTimeToSave
                )
            )
        )
        log.info("Save current state as CREATE_CONDITION_MET [$metadataToSave]")
        return ExecutionResult.Next(metadataToSave)
    }
}
