/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata

/**
 * States contain different business logics
 *
 * Supposed to be a stateless singleton so that it can be
 *  re-used by multiple state machine contexts
 * State metadata is supposed to be handled by the context like [SMStateMachine]
 */
interface State {
    /**
     * In single job run, this flag indicating whether we can
     * continue to execute the next state
     */
    val continuous: Boolean

    suspend fun execute(context: SMStateMachine): ExecutionResult

    /**
     * [Next]: go to the next vertical level state
     * [Stay]: if cont=true, try executing the next same level states
     *  like [CREATE_CONDITION_MET, DELETE_CONDITION_MET] in [SMState]
     *  if no more same level state, wait for next job run
     * [Failure]: caught exception, skipping to the START state
     */
    sealed class ExecutionResult {
        data class Next(val metadataToSave: SMMetadata) : ExecutionResult()
        data class Stay(val metadataToSave: SMMetadata? = null) : ExecutionResult()
        data class Failure(val ex: Exception) : ExecutionResult()
    }
}
