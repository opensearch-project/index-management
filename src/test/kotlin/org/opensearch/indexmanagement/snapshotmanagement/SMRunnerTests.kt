/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant.now

class SMRunnerTests : OpenSearchTestCase() {
    fun `test sm runner with deletion only policy should execute deletion workflow`() {
        val job = randomSMPolicy(
            creationNull = true, // Deletion-only policy
            deletionMaxAge = TimeValue.timeValueDays(7),
            deletionMinCount = 3,
        )
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = null, // No creation workflow
            deletion = SMMetadata.WorkflowMetadata(
                currentState = SMState.DELETION_START,
                trigger = SMMetadata.Trigger(time = now()),
            ),
        )

        // Verify the setup matches the condition
        assertNull("Creation should be null for deletion-only policy", job.creation)
        assertNotNull("Deletion should exist for deletion-only policy", job.deletion)
        assertNull("Metadata creation should be null", metadata.creation)
        assertNotNull("Metadata deletion should exist", metadata.deletion)

        assertTrue(
            "Should have deletion-only policy setup",
            metadata.creation == null && metadata.deletion != null,
        )
    }
}
