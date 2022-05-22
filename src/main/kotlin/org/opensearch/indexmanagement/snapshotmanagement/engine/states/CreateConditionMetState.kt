/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.Result
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import java.time.Instant
import java.time.Instant.now

object CreateConditionMetState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): Result {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        if (metadata.creation.started != null) {
            return Result.Stay()
        }

        val nextCreationTime = metadata.creation.trigger.time
        val nextCreationTimeToSave: Instant
        if (!now().isBefore(nextCreationTime)) {
            log.info("sm dev: Current time [${now()}] has passed nextCreationTime [$nextCreationTime]")
            nextCreationTimeToSave = getNextExecutionTime(job.creation.schedule, now())
        } else {
            log.info("sm dev: Current time [${now()}] has not passed nextCreationTime [$nextCreationTime]")
            // TODO SM dynamically update job start_time to avoid unnecessary job runs
            return Result.Stay()
        }

        val metadataToSave = SMMetadata.Builder(metadata)
            .currentState(SMState.CREATE_CONDITION_MET)
            .nextCreationTime(nextCreationTimeToSave)
            .build()
        log.info("sm dev: Save current state as CREATE_CONDITION_MET [$metadataToSave]")
        return Result.Next(metadataToSave)
    }
}
