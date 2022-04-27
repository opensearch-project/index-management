/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import org.opensearch.indexmanagement.snapshotmanagement.engine.states.CreateConditionMetState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.CreatingState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.DeleteConditionMetState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.DeletingState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.FinishedState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.StartState

enum class SMState(val instance: State) {
    START(StartState),
    CREATE_CONDITION_MET(CreateConditionMetState),
    DELETE_CONDITION_MET(DeleteConditionMetState),
    CREATING(CreatingState),
    DELETING(DeletingState),
    FINISHED(FinishedState),
}

/**
 * Transition has 2 directions
 * Vertical: WAITING to CONDITION_MET
 * Lateral: CREATE_CONDITION_MET and DELETE_CONDITION_MET, order matters
 */
val transitions: Map<SMState, List<SMState>> = mapOf(
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
