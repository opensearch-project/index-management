/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.rollover

import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData

class AttemptRolloverStep : Step(name) {

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
        const val name = "attempt_rollover"
        // TODO: fixme
        fun getFailedNoValidAliasMessage(indexName: String) = ""
        fun getPendingMessage(indexName: String) = ""
        fun getAlreadyRolledOverMessage(indexName: String, alias: String) = ""
        fun getSuccessDataStreamRolloverMessage(dataStreamName: String, indexName: String) = ""
        fun getSuccessMessage(indexName: String) = ""
        fun getFailedPreCheckMessage(indexName: String) = ""
        fun getSkipRolloverMessage(indexName: String) = ""
    }
}
