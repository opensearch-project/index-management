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
 * These workflows are pseudo parallel, which means one running shouldn't block the others.
 * These workflows have own set of metadata, so the executions shouldn't interfere.
 */
enum class WorkflowType {
    CREATION, // CREATE_CONDITION_MET, CREATING, part of FINISHED
    DELETION, // DELETE_CONDITION_MET, DELETING, part of FINISHED
}

/**
 * Result contains the executed metadata to persist and its special flags
 *
 * For the meaning of vertical, lateral, refer to [smTransitions].
 * [Next]: move to the next state in vertical direction. Save this currentState.
 * [Stay]: stay in this level, can execute the next lateral states if exists. Save the prevState.
 * [Failure]: caught non-retryable exception. Will reset the related metadata fields for this workflow.
 * [Retry]: caught retryable exception. If retry count exhausted, reset this workflow.
 *  When being used, remember to use [resetRetry] in metadata builder after passing the retry point.
 * [TimeLimitExceed]: the time limit of the workflow exceeds, reset this workflow.
 */
sealed class SMResult : State.Result() {
    data class Next(val metadataToSave: SMMetadata) : SMResult()
    data class Stay(val metadataToSave: SMMetadata) : SMResult()
    data class Failure(
        val metadataToSave: SMMetadata,
        val ex: Exception,
        val workflowType: WorkflowType,
        val notifiable: Boolean = false
    ) : SMResult()
    data class Retry(
        val metadataToSave: SMMetadata,
        val workflowType: WorkflowType
    ) : SMResult()
    data class TimeLimitExceed(
        val metadataToSave: SMMetadata,
        val workflowType: WorkflowType
    ) : SMResult()
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
