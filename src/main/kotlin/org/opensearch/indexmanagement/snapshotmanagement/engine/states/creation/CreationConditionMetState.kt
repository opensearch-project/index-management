/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation

import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.tryUpdatingNextExecutionTime
import java.time.Instant.now

object CreationConditionMetState : State {
    override val continuous = true

    @Suppress("ReturnCount")
    override suspend fun execute(context: SMStateMachine): SMResult {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder =
            SMMetadata.Builder(metadata)
                .workflow(WorkflowType.CREATION)

        if (job.creation == null) {
            log.warn("Policy creation config becomes null before trying to create snapshot. Reset.")
            return SMResult.Fail(
                metadataBuilder.resetCreation(), WorkflowType.CREATION, true,
            )
        }

        // if job.creation != null, then metadata.creation.trigger.time should already be
        // initialized or handled in handlePolicyChange before executing this state.
        val nextCreationTime = if (metadata.creation == null) {
            val nextTime = job.creation.schedule.getNextExecutionTime(now())
            nextTime?.let { metadataBuilder.setNextCreationTime(it) }
            nextTime
        } else {
            metadata.creation.trigger.time
        }

        nextCreationTime?.let { creationTime ->
            job.creation.schedule.let { schedule ->
                val updateNextTimeResult =
                    tryUpdatingNextExecutionTime(
                        metadataBuilder, creationTime, schedule, WorkflowType.CREATION, log,
                    )
                if (!updateNextTimeResult.updated) {
                    return SMResult.Stay(metadataBuilder)
                }
                metadataBuilder = updateNextTimeResult.metadataBuilder
            }
        }

        return SMResult.Next(metadataBuilder)
    }
}
