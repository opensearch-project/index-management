/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletion

import kotlinx.coroutines.runBlocking
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
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
import java.time.Instant.now

class DeletionFinishedStateTests : MocksTestCase() {
    fun `test pattern snapshots retrieval failure in finished state sets retry and fails`() =
        runBlocking {
            val startedSnapshot = "policy-snapshot-1"
            // First call (policy snapshots) succeeds with an empty list, second call (pattern) fails
            mockGetSnapshotsCallFirstSuccessThenFailure(
                firstResponse = mockGetSnapshotResponse(0),
                secondException = Exception("pattern getSnapshots failure"),
            )

            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = null,
                deletion = SMMetadata.WorkflowMetadata(
                    currentState = SMState.DELETING,
                    trigger = SMMetadata.Trigger(time = now()),
                    started = listOf(startedSnapshot),
                    latestExecution = randomLatestExecution(
                        startTime = Instant.now(),
                    ),
                ),
            )
            val job = randomSMPolicy(
                policyName = "test-policy",
                creationNull = true,
                snapshotPattern = "external-*",
            )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.DELETION_FINISHED.instance.execute(context)
            assertTrue("Execution results should be Failure.", result is SMResult.Fail)
            result as SMResult.Fail
            val metadataToSave = result.metadataToSave.build()
            assertEquals("Latest execution status is retrying", SMMetadata.LatestExecution.Status.RETRYING, metadataToSave.deletion!!.latestExecution!!.status)
        }
    fun `test deletion succeed`() =
        runBlocking {
            val snapshotName = "test_deletion_success"
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(0))

            val metadata =
                randomSMMetadata(
                    deletionCurrentState = SMState.DELETING,
                    startedDeletion = listOf(snapshotName),
                    deletionLatestExecution =
                    randomLatestExecution(
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

    fun `test deletion has not finished`() =
        runBlocking {
            val snapshotName = "test_deletion_not_finished"
            val snapshotInfo = mockSnapshotInfo(name = snapshotName)
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

            val metadata =
                randomSMMetadata(
                    deletionCurrentState = SMState.DELETING,
                    startedDeletion = listOf(snapshotName),
                    deletionLatestExecution =
                    randomLatestExecution(
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

    fun `test get snapshots exception in deletion`() =
        runBlocking {
            val snapshotName = "test_deletion_get_snapshots_exception"
            mockGetSnapshotsCall(exception = Exception())

            val metadata =
                randomSMMetadata(
                    deletionCurrentState = SMState.DELETING,
                    startedDeletion = listOf(snapshotName),
                    deletionLatestExecution =
                    randomLatestExecution(
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

    fun `test deletion time limit exceed`() =
        runBlocking {
            val snapshotName = "test_deletion_time_exceed"
            val snapshotInfo = mockSnapshotInfo(name = snapshotName)
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(snapshotInfo))

            val metadata =
                randomSMMetadata(
                    deletionCurrentState = SMState.DELETING,
                    startedDeletion = listOf(snapshotName),
                    deletionLatestExecution =
                    randomLatestExecution(
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

    fun `test deletion finished with pattern snapshots successful`() =
        runBlocking {
            val deletedSnapshot1 = "policy-snapshot-1"
            val deletedSnapshot2 = "external-backup-1"
            val snapshotNames = listOf(deletedSnapshot1, deletedSnapshot2)

            mockDeleteSnapshotCall(response = AcknowledgedResponse(true))
            // Mock the getSnapshots calls that DeletionFinishedState makes - return empty (snapshots deleted)
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(0)) // No snapshots found

            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = null, // No creation workflow
                deletion = SMMetadata.WorkflowMetadata(
                    currentState = SMState.DELETING,
                    trigger = SMMetadata.Trigger(time = now()),
                    started = snapshotNames,
                    latestExecution = randomLatestExecution(
                        startTime = Instant.now(),
                    ),
                ),
            )
            val job = randomSMPolicy(
                policyName = "test-policy",
                creationNull = true,
                deletionMaxAge = TimeValue.timeValueDays(7),
                deletionMinCount = 1,
                snapshotPattern = "external-*",
            )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.DELETION_FINISHED.instance.execute(context)
            assertTrue("Execution results should be Next.", result is SMResult.Next)
            result as SMResult.Next
            val metadataToSave = result.metadataToSave.build()
            assertNull("Started deletion should be reset to null.", metadataToSave.deletion?.started)
            assertEquals("Latest execution status is success", SMMetadata.LatestExecution.Status.SUCCESS, metadataToSave.deletion?.latestExecution?.status)
            assertNotNull("Latest execution status end_time should not be null", metadataToSave.deletion?.latestExecution?.endTime)
            assertNotNull("Latest execution status message should not be null", metadataToSave.deletion?.latestExecution?.info?.message)
        }

    fun `test deletion finished with no pattern snapshots found`() =
        runBlocking {
            // Test case where snapshots were started for deletion but are now gone (deletion finished)
            val deletedSnapshotNames = listOf("nonexistent-pattern-snapshot")

            mockDeleteSnapshotCall(response = AcknowledgedResponse(true))
            // Mock the getSnapshots calls that DeletionFinishedState makes - return empty (snapshots deleted)
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(0)) // No snapshots found

            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = null, // No creation workflow
                deletion = SMMetadata.WorkflowMetadata(
                    currentState = SMState.DELETING,
                    trigger = SMMetadata.Trigger(time = now()),
                    started = deletedSnapshotNames, // Snapshots that were started for deletion
                    latestExecution = randomLatestExecution(
                        startTime = Instant.now(),
                    ),
                ),
            )
            val job = randomSMPolicy(
                policyName = "test-policy",
                creationNull = true,
                deletionMaxAge = TimeValue.timeValueDays(7),
                deletionMinCount = 1,
                snapshotPattern = "nonexistent-*", // Pattern that matches nothing
            )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.DELETION_FINISHED.instance.execute(context)
            assertTrue("Execution results should be Next.", result is SMResult.Next)
            result as SMResult.Next
            val metadataToSave = result.metadataToSave.build()

            // Should complete successfully - snapshots that were started for deletion are now gone
            assertNull("Started deletion should be reset to null after completion.", metadataToSave.deletion?.started)
            assertEquals("Latest execution status is success", SMMetadata.LatestExecution.Status.SUCCESS, metadataToSave.deletion?.latestExecution?.status)
            assertNotNull("Latest execution status message should indicate deletion completed", metadataToSave.deletion?.latestExecution?.info?.message)
        }
}
