/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.updateNextExecutionTime

object CreateConditionMetState : State {

    override val continuous = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val job = context.job
        val metadata = context.metadata
        val log = context.log

        var metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(WorkflowType.CREATION)

        if (metadata.creation.started != null) {
            return SMResult.Stay(metadataBuilder.build())
        }

        val nextCreationTime = metadata.creation.trigger.time
        val result = updateNextExecutionTime(metadataBuilder, nextCreationTime, job.creation.schedule, WorkflowType.CREATION, log)
        if (!result.updated) return SMResult.Stay(metadataBuilder.build())
        metadataBuilder = result.metadataBuilder

        log.info("sm dev: Save current state as CREATE_CONDITION_MET [$metadataBuilder]")
        return SMResult.Next(metadataBuilder.build())
    }
}
