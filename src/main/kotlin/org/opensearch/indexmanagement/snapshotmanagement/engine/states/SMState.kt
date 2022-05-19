/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

enum class SMState(val instance: State) {
    START(StartState), // Dummy state
    CREATE_CONDITION_MET(CreateConditionMetState),
    DELETE_CONDITION_MET(DeleteConditionMetState),
    CREATING(CreatingState),
    DELETING(DeletingState),
    FINISHED(FinishedState),
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
