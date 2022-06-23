/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation.CreationConditionMetState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation.CreationFinishedState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation.CreatingState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation.CreationStartState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion.DeletionConditionMetState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion.DeletionFinishedState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion.DeletingState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion.DeletionStartState
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata

enum class SMState(val instance: State) {
    CREATION_START(CreationStartState),
    CREATION_CONDITION_MET(CreationConditionMetState),
    CREATING(CreatingState),
    CREATION_FINISHED(CreationFinishedState),
    DELETION_START(DeletionStartState),
    DELETION_CONDITION_MET(DeletionConditionMetState),
    DELETING(DeletingState),
    DELETION_FINISHED(DeletionFinishedState),
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
 * For the meaning of vertical, lateral, refer to [creationTransitions].
 * [Next]: move to the next state in vertical direction. Save this currentState.
 * [Stay]: stay in this level, can execute the next lateral states if exists. Save the prevState.
 * [Fail]: caught exception. Will start to retry. If retry count exhausted, reset this workflow.
 *   resetRetry is performed in Next and Stay.
 *   except the last one. Refer to [DeletingState]
 */
sealed class SMResult : State.Result() {
    data class Next(val metadataToSave: SMMetadata.Builder) : SMResult()
    data class Stay(val metadataToSave: SMMetadata.Builder) : SMResult()
    data class Fail(
        val metadataToSave: SMMetadata.Builder,
        val workflowType: WorkflowType,
        val forceReset: Boolean? = null, // situation like time limit exceed, we don't retry
    ) : SMResult()
}

// TODO SM enhance transition with predicate
/**
 * Transitions from current to next state vertically.
 * If there are multiple next states in lateral, these would be executed in sequence in order.
 */
val creationTransitions: Map<SMState, List<SMState>> = mapOf(
    SMState.CREATION_START to listOf(SMState.CREATION_CONDITION_MET),
    SMState.CREATION_CONDITION_MET to listOf(SMState.CREATING),
    SMState.CREATING to listOf(SMState.CREATION_FINISHED),
    SMState.CREATION_FINISHED to listOf(SMState.CREATION_START),
)

val deletionTransitions: Map<SMState, List<SMState>> = mapOf(
    SMState.DELETION_START to listOf(SMState.DELETION_CONDITION_MET),
    SMState.DELETION_CONDITION_MET to listOf(SMState.DELETING),
    SMState.DELETING to listOf(SMState.DELETION_FINISHED),
    SMState.DELETION_FINISHED to listOf(SMState.DELETION_START),
)
