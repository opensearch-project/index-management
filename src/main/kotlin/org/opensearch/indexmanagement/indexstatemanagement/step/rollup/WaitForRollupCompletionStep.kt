/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.rollup

import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData

class WaitForRollupCompletionStep : Step(name) {

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
        const val name = "wait_for_rollup_completion"
        // TODO: Fixme
        fun getJobFailedMessage(rollupId: String, indexName: String) = ""
        fun getJobProcessingMessage(rollupId: String, indexName: String) = ""
        fun getJobCompletionMessage(rollupId: String, indexName: String) = ""
    }
}
