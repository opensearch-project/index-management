/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata

object StartState : State {

    override val continuous: Boolean = false

    override suspend fun execute(context: SMStateMachine): ExecutionResult {
        val metadataToSave = SMMetadata.Builder(context.metadata)
            .currentState(SMState.START)
            .build()

        return ExecutionResult.Next(metadataToSave)
    }
}
