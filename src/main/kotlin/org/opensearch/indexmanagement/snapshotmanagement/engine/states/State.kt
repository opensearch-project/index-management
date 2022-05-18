/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.jobscheduler.spi.ScheduledJobParameter

/**
 * States contain the action to execute
 *
 * Execution metadata can be handled by the context object. e.g. [SMStateMachine]
 */
interface State {
    /**
     * In single [ScheduledJobParameter] run, this flag indicates
     * whether to continue executing next state
     */
    val continuous: Boolean

    suspend fun execute(context: SMStateMachine): ExecutionResult

    /**
     * [Next]: move to the next state in vertical direction
     *  For vertical, lateral's meaning, refer to [smTransitions]
     * [Stay]: stay in this lateral level, try executing the next lateral states
     *  if no more same level state, wait for next job run
     * [Failure]: caught exception, will skip to the START state
     */
    sealed class ExecutionResult {
        data class Next(val metadataToSave: SMMetadata) : ExecutionResult()
        data class Stay(val metadataToSave: SMMetadata? = null) : ExecutionResult()
        data class Failure(val ex: Exception) : ExecutionResult()
    }
}
