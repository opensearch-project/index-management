/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.creation

import kotlinx.coroutines.runBlocking
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.MocksTestCase
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockInProgressSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomLatestExecution
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import java.time.Instant

class CreationFinishedStateTests : MocksTestCase() {
    fun `test creation end successful`() = runBlocking {
        val snapshotName = "test_creation_succeed"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = Instant.now(),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.CREATION_FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        val metadataToSave = result.metadataToSave.build()
        assertNull("Started creation should be reset to null.", metadataToSave.creation.started)
        assertEquals("Latest execution status is success", SMMetadata.LatestExecution.Status.SUCCESS, metadataToSave.creation.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", metadataToSave.creation.latestExecution!!.endTime)
        assertNotNull("Latest execution status message should not be null", metadataToSave.creation.latestExecution!!.info!!.message)
    }

    fun `test creation still in progress`() = runBlocking {
        val snapshotName = "test_creation_in_progress"
        val snapshotInfo = mockInProgressSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = Instant.now(),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.CREATION_FINISHED.instance.execute(context)
        assertTrue("Execution results should be Stay.", result is SMResult.Stay)
        result as SMResult.Stay
        val metadataToSave = result.metadataToSave.build()
        assertEquals("Started creation should not be reset.", snapshotName, metadataToSave.creation.started!!.first())
    }

    fun `test creation end not successful`() = runBlocking {
        val snapshotName = "test_creation_end_with_failed_state"
        val snapshotInfo = mockSnapshotInfo(name = snapshotName, reason = "failed state")
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = Instant.now(),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.CREATION_FINISHED.instance.execute(context)
        assertTrue("Execution results should be Fail.", result is SMResult.Fail)
        result as SMResult.Fail
        val metadataToSave = result.metadataToSave.build()
        assertNull("Started creation should be reset to null.", metadataToSave.creation.started)
        assertEquals("Latest execution status is failed", SMMetadata.LatestExecution.Status.FAILED, metadataToSave.creation.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", metadataToSave.creation.latestExecution!!.endTime)
        assertNotNull("Latest execution status cause should not be null", metadataToSave.creation.latestExecution!!.info!!.cause)
    }

    fun `test get snapshots exception in creation`() = runBlocking {
        val snapshotName = "test_creation_get_snapshots_exception"
        mockGetSnapshotsCall(exception = Exception())

        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = Instant.now(),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.CREATION_FINISHED.instance.execute(context)
        assertTrue("Execution results should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        val metadataToSave = result.metadataToSave.build()
        assertEquals("Latest execution status is retrying", SMMetadata.LatestExecution.Status.RETRYING, metadataToSave.creation.latestExecution!!.status)
        assertNull("Latest execution status end_time should be null", metadataToSave.creation.latestExecution!!.endTime)
        assertNotNull("Latest execution status info should not be null", metadataToSave.creation.latestExecution!!.info)
    }

    fun `test get snapshots empty in creation`() = runBlocking {
        val snapshotName = "test_creation_get_snapshots_empty"
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))

        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = Instant.now(),
            ),
        )
        val job = randomSMPolicy(policyName = "daily-snapshot")
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.CREATION_FINISHED.instance.execute(context)
        assertTrue("Execution results should be Next.", result is SMResult.Next)
        result as SMResult.Next
        val metadataToSave = result.metadataToSave.build()
        assertEquals("Latest execution status is success", SMMetadata.LatestExecution.Status.SUCCESS, metadataToSave.creation.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", metadataToSave.creation.latestExecution!!.endTime)
        assertNotNull("Latest execution status message should not be null", metadataToSave.creation.latestExecution!!.info!!.message)
    }

    fun `test creation time limit exceed`() = runBlocking {
        val snapshotName = "test_creation_time_exceed"
        val snapshotInfo = mockInProgressSnapshotInfo(name = snapshotName)
        mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

        val metadata = randomSMMetadata(
            creationCurrentState = SMState.CREATING,
            startedCreation = snapshotName,
            creationLatestExecution = randomLatestExecution(
                startTime = Instant.now().minusSeconds(10),
            ),
        )
        val job = randomSMPolicy(
            policyName = "daily-snapshot",
            creationTimeLimit = TimeValue.timeValueSeconds(5)
        )
        val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

        val result = SMState.CREATION_FINISHED.instance.execute(context)
        assertTrue("Execution results should be Failure.", result is SMResult.Fail)
        result as SMResult.Fail
        val metadataToSave = result.metadataToSave.build()
        assertTrue(result.forceReset!!)
        assertEquals("Latest execution status is time limit exceed", SMMetadata.LatestExecution.Status.TIME_LIMIT_EXCEEDED, metadataToSave.creation.latestExecution!!.status)
        assertNotNull("Latest execution status end_time should not be null", metadataToSave.creation.latestExecution!!.endTime)
        assertNotNull("Latest execution status cause should not be null", metadataToSave.creation.latestExecution!!.info!!.cause)
    }
}
