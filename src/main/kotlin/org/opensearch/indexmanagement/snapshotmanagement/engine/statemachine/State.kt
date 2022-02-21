/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

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

    /**
     * @throws StateMachineExecutionException
     * @return if true, save metadata and go to the next level state
     *  if false, can still try executing the same level states
     *   like [CREATE_CONDITION_MET, DELETE_CONDITION_MET]
     *   if no more same level state, wait for next job run
     *  false doesn't mean state execution failure
     */
    suspend fun execute(context: SMStateMachine): Boolean
}

/**
 * Represent state execution failure
 */
class StateMachineExecutionException(message: String? = null, cause: Throwable? = null) : Exception(message, cause)
