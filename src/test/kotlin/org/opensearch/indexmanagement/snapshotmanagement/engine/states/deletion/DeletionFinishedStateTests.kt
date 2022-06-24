/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import kotlinx.coroutines.runBlocking
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.MocksTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomLatestExecution
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import java.time.Instant

class DeletionFinishedStateTests : MocksTestCase() {
    fun `test deletion succeed`() = runBlocking {
        val snapshotName = "test_deletion_success"
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))

        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            deletionLatestExecution = randomLatestExecution(
                startTime = Instant.now().minusSeconds(50),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETION_FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        val metadataToSave = result.metadataToSave.build()
        assertNull("Started deletion should be reset to null.", metadataToSave.deletion!!.started)
        assertEquals("Latest execution status is success", SMMetadata.LatestExecution.Status.SUCCESS, metadataToSave.deletion!!.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", metadataToSave.deletion!!.latestExecution!!.endTime)
        assertNotNull("Latest execution status message should not be null", metadataToSave.deletion!!.latestExecution!!.info!!.message)
    }

    fun `test deletion has not finished`() = runBlocking {
        val snapshotName = "test_deletion_not_finished"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            deletionLatestExecution = randomLatestExecution(
                startTime = Instant.now().minusSeconds(50),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETION_FINISHED.instance.execute(context)
        assertTrue("Execution results should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        val metadataToSave = result.metadataToSave.build()
        assertNotNull("Started deletion should not be reset.", metadataToSave.deletion!!.started)
        assertEquals("Latest execution status is in_progress", SMMetadata.LatestExecution.Status.IN_PROGRESS, metadataToSave.deletion!!.latestExecution!!.status)
        assertNull("Latest execution status end_time should be null", metadataToSave.deletion!!.latestExecution!!.endTime)
    }

    fun `test get snapshots exception in deletion`() = runBlocking {
        val snapshotName = "test_deletion_get_snapshots_exception"
        mockGetSnapshotsCall(exception = Exception())

        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            deletionLatestExecution = randomLatestExecution(
                startTime = Instant.now().minusSeconds(50),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETION_FINISHED.instance.execute(context)
        assertTrue("Execution results should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        val metadataToSave = result.metadataToSave.build()
        assertEquals("Latest execution status is retrying", SMMetadata.LatestExecution.Status.RETRYING, metadataToSave.deletion!!.latestExecution!!.status)
        assertNull("Latest execution status end_time should be null", metadataToSave.deletion!!.latestExecution!!.endTime)
        assertNotNull("Latest execution status info should not be null", metadataToSave.deletion!!.latestExecution!!.info)
    }

    fun `test deletion time limit exceed`() = runBlocking {
        val snapshotName = "test_deletion_time_exceed"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            deletionCurrentState = SMState.DELETING,
            startedDeletion = listOf(snapshotName),
            deletionLatestExecution = randomLatestExecution(
                startTime = Instant.now().minusSeconds(50),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot", deletionTimeLimit = TimeValue.timeValueSeconds(5))
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.DELETION_FINISHED.instance.execute(context)
        assertTrue("Execution results should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        val metadataToSave = result.metadataToSave.build()
        assertTrue(result.forceReset!!)
        assertEquals("Latest execution status is time limit exceed", SMMetadata.LatestExecution.Status.TIME_LIMIT_EXCEEDED, metadataToSave.deletion!!.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", metadataToSave.deletion!!.latestExecution!!.endTime)
        assertNotNull("Latest execution status cause should not be null", metadataToSave.deletion!!.latestExecution!!.info!!.cause)
    }
}
