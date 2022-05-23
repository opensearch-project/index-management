/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata

enum class SMState(val instance: State) {
    START(StartState), // Dummy state
    CREATE_CONDITION_MET(CreateConditionMetState),
    DELETE_CONDITION_MET(DeleteConditionMetState),
    CREATING(CreatingState),
    DELETING(DeletingState),
    FINISHED(FinishedState),
}

/**
 * The workflows are pseudo parallel, which means one running won't block the others
 */
enum class WorkflowType {
    CREATION, // CREATE_CONDITION_MET, CREATING, part of FINISHED
    DELETION, // DELETE_CONDITION_MET, DELETING, part of FINISHED
}

/**
 * For the meaning of vertical, lateral, refer to [smTransitions].
 * [Next]: move to the next state in vertical direction. Save currentState to metadata.
 * [Stay]: stay in this level, can execute the next lateral states if exists. Save prevState to metadata if needed.
 * [Failure]: caught non-retryable exception and will decide whether to show to the user.
 *  Reset this workflow.
 * [Retry]: caught retryable exception. If retry count exhausted, reset this workflow.
 * [TimeLimitExceed]: the time limit of the workflow exceeds, reset this workflow.
 */
sealed class SMResult : State.Result() {
    data class Next(val metadataToSave: SMMetadata) : SMResult()
    data class Stay(val metadataToSave: SMMetadata? = null) : SMResult()
    data class Failure(val ex: Exception, val workflowType: WorkflowType, val notifiable: Boolean = false) : SMResult()
    data class Retry(val workflowType: WorkflowType) : SMResult()
    data class TimeLimitExceed(val workflowType: WorkflowType) : SMResult()
}

/**
 * Transitions from current to next state
 *
 * Vertical example: WAITING to CONDITION_MET
 * Lateral example: CREATE_CONDITION_MET and DELETE_CONDITION_MET, order matters
 */
val smTransitions: Map<SMState, List<SMState>> = mapOf(
    SMState.START to listOf(
        SMState.CREATE_CONDITION_MET,
        SMState.DELETE_CONDITION_MET,
    ),
    SMState.CREATE_CONDITION_MET to listOf(SMState.CREATING),
    SMState.DELETE_CONDITION_MET to listOf(SMState.DELETING),
    SMState.CREATING to listOf(
        SMState.DELETE_CONDITION_MET,
        SMState.FINISHED,
    ),
    SMState.DELETING to listOf(
        SMState.CREATE_CONDITION_MET,
        SMState.FINISHED,
    ),
    SMState.FINISHED to listOf(SMState.START),
)
