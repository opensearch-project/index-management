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

    override suspend fun execute(context: SMStateMachine): SMResult {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.DELETION)

        if (job.deletion == null) {
            log.warn("Policy deletion config is null before checking if delete schedule met. Reset.")
            return SMResult.Fail(
                metadataBuilder.resetDeletion(),
                WorkflowType.DELETION, forceReset = true
            )
        }

        // if job.deletion != null, then metadata.deletion.trigger.time should be init
        //  or handled during policy change
        val nextDeletionTime = metadata.deletion?.trigger?.time ?: job.deletion.schedule.getNextExecutionTime(now())
        val result = tryUpdatingNextExecutionTime(
            metadataBuilder, nextDeletionTime, job.deletion.schedule,
            WorkflowType.DELETION, log
        )
        if (!result.updated) return SMResult.Stay(metadataBuilder)
        metadataBuilder = result.metadataBuilder

        return SMResult.Next(metadataBuilder)
    }
}
