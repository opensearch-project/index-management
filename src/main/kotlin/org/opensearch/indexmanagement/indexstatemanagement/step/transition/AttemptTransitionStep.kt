/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.transition

import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData

class AttemptTransitionStep : Step(name) {
    override suspend fun execute(): Step {
        TODO("Not yet implemented")
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        TODO("Not yet implemented")
    }

    override fun isIdempotent(): Boolean {
        TODO("Not yet implemented")
    }

    companion object {
        const val name = "attempt_transition_step"
        // TODO: Fix me
        fun getSuccessMessage(indexName: String, state: String) = ""
        fun getEvaluatingMessage(indexName: String) = ""
        fun getFailedRolloverDateMessage(indexName: String) = ""
    }
}
