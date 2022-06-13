/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata

object StartState : State {

    override val continuous: Boolean = false

    override suspend fun execute(context: SMStateMachine): SMResult {
        val metadataToSave = SMMetadata.Builder(context.metadata)
            .setCurrentState(SMState.START)
            .build()

        return SMResult.Next(metadataToSave)
    }
}
