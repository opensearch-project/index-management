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
     * For the meaning of vertical, lateral, refer to [smTransitions].
     * [Next]: move to the next state in vertical direction.
     * [Stay]: stay in this level, can execute the next lateral states if exists,
     * [Failure]: caught exception, show the exception to user in metadata.info.
     *  Reset metadata and skip the workflow to next execution.
     */
    sealed class ExecutionResult {
        data class Next(val metadataToSave: SMMetadata) : ExecutionResult()
        data class Stay(val metadataToSave: SMMetadata? = null) : ExecutionResult()
        data class Failure(val ex: Exception, val workflowType: SMMetadata.WorkflowType, val reset: Boolean) : ExecutionResult()
        data class TimeLimitExceed(val workflowType: SMMetadata.WorkflowType) : ExecutionResult()
    }
}
