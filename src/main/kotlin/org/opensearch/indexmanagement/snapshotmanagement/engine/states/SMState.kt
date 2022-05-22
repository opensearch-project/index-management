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

enum class WorkflowType {
    CREATION,
    DELETION,
}

/**
 * For the meaning of vertical, lateral, refer to [smTransitions].
 * [Next]: move to the next state in vertical direction.
 * [Stay]: stay in this level, can execute the next lateral states if exists.
 * [Failure]: caught exception and decide whether to show to the user. always reset the workflow.
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
 * Vertical: WAITING to CONDITION_MET
 * Lateral: CREATE_CONDITION_MET and DELETE_CONDITION_MET, order matters
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
