/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State

/**
 * Dummy start state of snapshot management state machine
 */
object StartState : State {

    override val continuous: Boolean = false

    override suspend fun execute(context: SMStateMachine): Boolean {
        context.metadataToSave = context.metadata.copy(
            currentState = SMState.START.toString()
        )
        context.log.info("Save current state as WAITING")

        return true
    }
}
