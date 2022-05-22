/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.test.OpenSearchTestCase

class StartStateTests : OpenSearchTestCase() {
    fun `test start state execution`() = runBlocking {
        val metadata = randomSMMetadata(
            currentState = SMState.FINISHED
        )
    }
}
