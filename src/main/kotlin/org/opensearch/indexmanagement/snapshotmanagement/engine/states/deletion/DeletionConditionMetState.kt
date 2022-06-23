/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.tryUpdatingNextExecutionTime
import java.time.Instant.now

// check the status of creating, deleting snapshot
object DeletionConditionMetState : State {

    override val continuous = true

    @Suppress("ReturnCount")
    override suspend fun execute(context: SMStateMachine): SMResult {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.DELETION)

        if (job.deletion == null) {
            log.warn("Policy deletion config becomes null before checking if delete schedule met. Reset.")
            return SMResult.Fail(
                metadataBuilder.resetDeletion(), WorkflowType.DELETION, forceReset = true
            )
        }

        // if job.deletion != null, then metadata.deletion.trigger.time should already be
        //  initialized or handled in handlePolicyChange before executing this state.
        val nextDeletionTime = if (metadata.deletion == null) {
            val nextTime = job.deletion.schedule.getNextExecutionTime(now())
            metadataBuilder.setNextDeletionTime(nextTime)
            nextTime
        } else {
            metadata.deletion.trigger.time
        }
        val updateNextTimeResult = tryUpdatingNextExecutionTime(
            metadataBuilder, nextDeletionTime, job.deletion.schedule, WorkflowType.DELETION, log
        )
        if (!updateNextTimeResult.updated) {
            return SMResult.Stay(metadataBuilder)
        }
        metadataBuilder = updateNextTimeResult.metadataBuilder

        return SMResult.Next(metadataBuilder)
    }
}
