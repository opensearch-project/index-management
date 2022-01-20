/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.snapshot

import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData

// TODO remove this suppression when refactoring is done
@Suppress("UnusedPrivateMember", "FunctionOnlyReturningConstant")
class AttemptSnapshotStep : Step(name) {

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
        const val name = "attempt_snapshot"
        // TODO: Fix me
        fun getSuccessMessage(indexName: String) = ""
        fun getBlockedMessage(denyList: List<String>, repoName: String, indexName: String) = ""
    }
}
