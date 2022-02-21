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

        if (metadata.creating != null) {
            log.info("There is a creating snapshot ${metadata.creating}.")
            return false
        }

        val nextCreationTimeToSave: Instant

        if (!now().isBefore(metadata.nextCreationTime)) {
            log.info("current time [${now()}] has passed nextCreationTime [${metadata.nextCreationTime}]")
            nextCreationTimeToSave = getNextExecutionTime(job.createSchedule, now())
        } else {
            log.info("current time [${now()}] has not passed nextCreationTime [${metadata.nextCreationTime}]")
            return false
        }

        context.metadataToSave = metadata.copy(
            currentState = SMState.CREATE_CONDITION_MET.toString(),
            nextCreationTime = nextCreationTimeToSave
        )
        log.info("Save current state as CREATE_CONDITION_MET [${context.metadataToSave}]")
        return true
    }
}
