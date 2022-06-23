/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import kotlinx.coroutines.runBlocking
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.MocksTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotsResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomLatestExecution
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.randomSnapshotName
import java.time.Instant.now

class DeletingStateTests : MocksTestCase() {

    fun `test snapshots exceed max count`() = runBlocking {
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(11))
        mockDeleteSnapshotCall(response = AcknowledgedResponse(true))

        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_CONDITION_MET,
        )
        val job = randomSMPolicy(
            policyName = "daily-snapshot",
            deletionMaxCount = 10,
        )
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        val metadataToSave = result.metadataToSave.build()
        assertNotNull("Deletion started field is initialized.", metadataToSave.deletion!!.started)
        assertEquals(1, metadataToSave.deletion?.started!!.size)
        assertEquals("Latest execution status is in_progress", SMMetadata.LatestExecution.Status.IN_PROGRESS, metadataToSave.deletion!!.latestExecution!!.status)
    }

    fun `test snapshots exceed max age`() = runBlocking {
        val oldSnapshot = mockSnapshotInfo(name = "old_snapshot", startTime = now().minusSeconds(2 * 60).toEpochMilli())
        val newSnapshot = mockSnapshotInfo(name = "new_snapshot", startTime = now().toEpochMilli())
        mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(oldSnapshot, newSnapshot, newSnapshot)))
        mockDeleteSnapshotCall(response = AcknowledgedResponse(true))

        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_CONDITION_MET,
        )
        val job = randomSMPolicy(
            policyName = "daily-snapshot",
            deletionMaxAge = TimeValue.timeValueMinutes(1),
            deletionMinCount = 2,
        )
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        val metadataToSave = result.metadataToSave.build()
        assertNotNull("Deletion started field is initialized.", metadataToSave.deletion!!.started)
        assertEquals(1, metadataToSave.deletion?.started!!.size)
        assertEquals("old_snapshot", metadataToSave.deletion!!.started!!.first())
        assertEquals("Latest execution status is in_progress", SMMetadata.LatestExecution.Status.IN_PROGRESS, metadataToSave.deletion!!.latestExecution!!.status)
    }

    fun `test snapshots exceed max age but need to remain min count`() = runBlocking {
        val oldSnapshot = mockSnapshotInfo(name = "old_snapshot", startTime = now().minusSeconds(2 * 60).toEpochMilli())
        val newSnapshot = mockSnapshotInfo(name = "new_snapshot", startTime = now().toEpochMilli())
        mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(oldSnapshot, newSnapshot)))

        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_CONDITION_MET,
        )
        val job = randomSMPolicy(
            policyName = "daily-snapshot",
            deletionMaxAge = TimeValue.timeValueMinutes(1),
            deletionMinCount = 2,
        )
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        val metadataToSave = result.metadataToSave.build()
        assertNull("Deletion started field should not be initialized.", metadataToSave.deletion!!.started)
        assertNull("Latest execution should not be initialized", metadataToSave.deletion!!.latestExecution)
    }

    fun `test snapshots min count check won't get negative deletion count`() = runBlocking {
        val oldSnapshot = mockSnapshotInfo(name = "old_snapshot", startTime = now().minusSeconds(2 * 60).toEpochMilli())
        mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(oldSnapshot)))

        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_CONDITION_MET,
        )
        val job = randomSMPolicy(
            policyName = "daily-snapshot",
            deletionMaxAge = TimeValue.timeValueMinutes(1),
            deletionMinCount = 2,
        )
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Next.", result is SMResult.Next)
        result as SMResult.Next
        val metadataToSave = result.metadataToSave.build()
        assertNull("Deletion started field should not be initialized.", metadataToSave.deletion!!.started)
        assertNull("Latest execution should not be initialized", metadataToSave.deletion!!.latestExecution)
    }

    fun `test delete snapshot exception`() = runBlocking {
        val ex = Exception()
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(11))
        mockDeleteSnapshotCall(exception = ex)

        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_CONDITION_MET,
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        val metadataToSave = result.metadataToSave.build()
        assertNull("Deletion started field should not be initialized.", metadataToSave.deletion!!.started)
        assertEquals("Latest execution status is retrying", SMMetadata.LatestExecution.Status.RETRYING, metadataToSave.deletion!!.latestExecution!!.status)
        assertNotNull("Latest execution info should not be null", metadataToSave.deletion!!.latestExecution!!.info)
    }

    fun `test get snapshots exception`() = runBlocking {
        val ex = Exception()
        mockGetSnapshotsCall(exception = ex)

        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_CONDITION_MET,
            deletionLatestExecution = randomLatestExecution(
                startTime = now().minusSeconds(10),
            )
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Fail.", result is SMResult.Fail)
        result as SMResult.Fail
        val metadataToSave = result.metadataToSave.build()
        assertNull("Deletion started field should not be initialized.", metadataToSave.deletion!!.started)
        assertEquals("Latest execution status is retrying", SMMetadata.LatestExecution.Status.RETRYING, metadataToSave.deletion!!.latestExecution!!.status)
        assertNotNull("Latest execution info should not be null", metadataToSave.deletion!!.latestExecution!!.info)
    }

    fun `test policy deletion is null`() = runBlocking {
        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETION_CONDITION_MET,
            startedDeletion = listOf(randomSnapshotName()),
        )
        val job = randomSMPolicy(
            deletionNull = true
        )
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETING.instance.execute(context)
        assertTrue("Execution result should be Fail.", result is SMResult.Fail)
        result as SMResult.Fail
        val metadataToSave = result.metadataToSave.build()
        assertNull("Deletion metadata should be null.", metadataToSave.deletion)
    }
}
