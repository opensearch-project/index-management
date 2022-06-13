/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.updateNextExecutionTime
import java.time.Instant.now

// check the status of creating, deleting snapshot
object DeleteConditionMetState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.DELETION)

        if (job.deletion == null || metadata.deletion?.started != null) {
            return SMResult.Stay(metadataBuilder.build())
        }

        // if job.deletion != null, then metadata.deletion.trigger.time should be init
        //  or handled during policy change
        val nextDeletionTime = metadata.deletion?.trigger?.time ?: job.deletion.schedule.getNextExecutionTime(now())
        val result = updateNextExecutionTime(metadataBuilder, nextDeletionTime, job.deletion.schedule, WorkflowType.DELETION, log)
        if (!result.updated) return SMResult.Stay(metadataBuilder.build())
        metadataBuilder = result.metadataBuilder

        return SMResult.Next(metadataBuilder.build())
    }
}
