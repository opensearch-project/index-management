/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import java.time.Instant
import java.time.Instant.now

object CreateConditionMetState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): Boolean {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        if (metadata.creation.started != null) {
            log.info("There is already a snapshot being created: ${metadata.creation.started}.")
            return false
        }

        val nextCreationTime = metadata.creation.trigger.nextExecutionTime
        val nextCreationTimeToSave: Instant
        if (!now().isBefore(nextCreationTime)) {
            log.info("current time [${now()}] has passed nextCreationTime [$nextCreationTime]")
            nextCreationTimeToSave = getNextExecutionTime(job.creation.schedule, now())
        } else {
            log.info("current time [${now()}] has not passed nextCreationTime [$nextCreationTime]")
            // TODO update job start_time
            return false
        }

        context.metadataToSave = metadata.copy(
            currentState = SMState.CREATE_CONDITION_MET.toString(),
            creation = metadata.creation.copy(
                trigger = metadata.creation.trigger.copy(
                    nextExecutionTime = nextCreationTimeToSave
                )
            )
        )
        log.info("Save current state as CREATE_CONDITION_MET [${context.metadataToSave}]")
        return true
    }
}
