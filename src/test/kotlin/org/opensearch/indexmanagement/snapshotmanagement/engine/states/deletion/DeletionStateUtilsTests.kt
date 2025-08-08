/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import kotlinx.coroutines.runBlocking
import org.opensearch.indexmanagement.MocksTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotsResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import java.time.Instant

class DeletionStateUtilsTests : MocksTestCase() {
    fun `test getPatternSnapshots returns empty when no pattern`() = runBlocking {
        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = null,
            deletion = SMMetadata.WorkflowMetadata(
                currentState = org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState.DELETION_START,
                trigger = SMMetadata.Trigger(time = Instant.now()),
            ),
        )
        val job = randomSMPolicy(creationNull = true, snapshotPattern = null)
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val builder = SMMetadata.Builder(metadata).workflow(org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType.DELETION)
        val res = DeletionStateUtils.getPatternSnapshots(context, builder)
        requireNotNull(res)
        val (patternSnapshots, updatedBuilder) = res
        assertTrue(patternSnapshots.isEmpty())
        // metadata builder should be unchanged
        assertEquals(builder.build(), updatedBuilder.build())
    }
    fun `test getPatternSnapshots returns snapshots when pattern present`() = runBlocking {
        val snapshot = mockSnapshotInfo(name = "external-1")
        mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(snapshot)))

        val metadata = SMMetadata(
            policySeqNo = 1L,
            policyPrimaryTerm = 1L,
            creation = null,
            deletion = SMMetadata.WorkflowMetadata(
                currentState = org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState.DELETION_START,
                trigger = SMMetadata.Trigger(time = Instant.now()),
            ),
        )
        val job = randomSMPolicy(creationNull = true, snapshotPattern = "external-*")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val builder = SMMetadata.Builder(metadata).workflow(org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType.DELETION)
        val res = DeletionStateUtils.getPatternSnapshots(context, builder)
        requireNotNull(res)
        val (patternSnapshots, _) = res
        assertEquals(1, patternSnapshots.size)
        assertEquals("external-1", patternSnapshots.first().snapshotId().name)
    }
}
