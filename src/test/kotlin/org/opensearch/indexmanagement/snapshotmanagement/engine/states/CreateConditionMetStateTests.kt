package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant.now

class CreateConditionMetStateTests : OpenSearchTestCase() {

    fun `test next creation time met`() {
        // metadata
        val metadata = SMMetadata(
            policySeqNo = randomNonNegativeLong(),
            policyPrimaryTerm = randomNonNegativeLong(),
            currentState = SMState.START.toString(),
            apiCalling = false,
            creation = SMMetadata.Creation(
                trigger = SMMetadata.Trigger(
                    nextExecutionTime = now().minusSeconds(5)
                )
            ),
            deletion = SMMetadata.Deletion(
                trigger = SMMetadata.Trigger(
                    nextExecutionTime = now().minusSeconds(5)
                )
            ),
        )
        // job
    }
}
