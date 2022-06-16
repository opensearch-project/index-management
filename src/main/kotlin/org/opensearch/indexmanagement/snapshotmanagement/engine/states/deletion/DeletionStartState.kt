/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata

object DeletionStartState : State {

    override val continuous: Boolean = true

    override suspend fun execute(context: SMStateMachine): SMResult {
        val metadataToSave = SMMetadata.Builder(context.metadata)
            .workflow(WorkflowType.DELETION)
            .setCurrentState(SMState.DELETION_START)

        return SMResult.Next(metadataToSave)
    }
}
