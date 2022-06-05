/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import java.time.Instant
import java.time.Instant.now

// check the status of creating, deleting snapshot
object DeleteConditionMetState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        val metadataBuilder = SMMetadata.Builder(metadata)

        if (metadata.deletion.started != null) {
            return SMResult.Stay(metadataBuilder.build())
        }

        val nextDeletionTime = metadata.deletion.trigger.time
        val nextDeletionTimeToSave: Instant
        if (!now().isBefore(nextDeletionTime)) {
            log.info("sm dev current time [${now()}] has passed nextDeletionTime [$nextDeletionTime]")
            nextDeletionTimeToSave = getNextExecutionTime(job.deletion.schedule, now())
            metadataBuilder.setNextDeletionTime(nextDeletionTimeToSave)
        } else {
            log.info("sm dev: current time [${now()}] has not passed nextDeletionTime [$nextDeletionTime]")
            // TODO SM dynamically update job start_time to avoid unnecessary job runs
            return SMResult.Stay(metadataBuilder.build())
        }

        return SMResult.Next(metadataBuilder.build())
    }
}
