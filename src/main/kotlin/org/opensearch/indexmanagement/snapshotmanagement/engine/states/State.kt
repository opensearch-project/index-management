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

    suspend fun execute(context: SMStateMachine): Result

    /**
     * For the meaning of vertical, lateral, refer to [smTransitions].
     * [Next]: move to the next state in vertical direction.
     * [Stay]: stay in this level, can execute the next lateral states if exists.
     * [Failure]: caught exception and decide whether to show to the user. always reset the workflow.
     */
    sealed class Result {
        data class Next(val metadataToSave: SMMetadata) : Result()
        data class Stay(val metadataToSave: SMMetadata? = null) : Result()
        data class Failure(val ex: Exception, val workflowType: SMMetadata.WorkflowType, val notifiable: Boolean = false) : Result()
        data class Retry(val workflowType: SMMetadata.WorkflowType) : Result()
        data class TimeLimitExceed(val workflowType: SMMetadata.WorkflowType) : Result()
    }
}
