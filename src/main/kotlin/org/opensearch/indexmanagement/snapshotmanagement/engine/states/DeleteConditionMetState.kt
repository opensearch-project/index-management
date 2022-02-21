/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.apache.logging.log4j.LogManager
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import java.time.Instant

// check the status of creating, deleting snapshot
object DeleteConditionMetState : State {

    private val log = LogManager.getLogger(javaClass)

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): Boolean {
        val job = context.job
        val metadata = context.metadata

        if (metadata.deleting != null) {
            log.info("There is deleting snapshot ${metadata.deleting}.")
            return false
        }

        val nextDeletionTimeToSave: Instant

        if (!Instant.now().isBefore(metadata.nextDeletionTime)) {
            log.info("current time [${Instant.now()}] has passed nextDeletionTime [${metadata.nextDeletionTime}]")
            nextDeletionTimeToSave = getNextExecutionTime(job.deleteSchedule, Instant.now())
        } else {
            log.info("current time [${Instant.now()}] has not passed nextDeletionTime [${metadata.nextDeletionTime}]")
            return false
        }

        context.metadataToSave = metadata.copy(
            currentState = SMState.CREATE_CONDITION_MET.toString(),
            nextDeletionTime = nextDeletionTimeToSave
        )
        log.info("Save current state as DELETE_CONDITION_MET [${context.metadataToSave}]")
        return true
    }
}
