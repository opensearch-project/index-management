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
import org.opensearch.indexmanagement.snapshotmanagement.mockGetSnapshotsResponse
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomLatestExecution
import org.opensearch.indexmanagement.snapshotmanagement.randomSMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.randomSnapshotName
import java.time.Instant.now

class DeletingStateTests : MocksTestCase() {
    fun `test pattern snapshots retrieval failure sets retry and fails`() =
        runBlocking {
            val policySnapshot = mockSnapshotInfo(name = "policy-snapshot-1", startTime = now().minusSeconds(8 * 24 * 3600).toEpochMilli(), policyName = "test-policy")
            // First getSnapshots (policy-created snapshots) succeeds, second (pattern) fails
            mockGetSnapshotsCallFirstSuccessThenFailure(
                firstResponse = mockGetSnapshotsResponse(listOf(policySnapshot)),
                secondException = Exception("pattern getSnapshots failure"),
            )

            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = null,
                deletion = SMMetadata.WorkflowMetadata(
                    currentState = SMState.DELETION_CONDITION_MET,
                    trigger = SMMetadata.Trigger(time = now()),
                ),
            )
            val job = randomSMPolicy(
                policyName = "test-policy",
                creationNull = true,
                deletionMaxAge = org.opensearch.common.unit.TimeValue.timeValueDays(7),
                deletionMinCount = 1,
                snapshotPattern = "external-*",
            )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.DELETING.instance.execute(context)
            assertTrue("Execution result should be Fail.", result is SMResult.Fail)
            result as SMResult.Fail
            val metadataToSave = result.metadataToSave.build()
            assertEquals("Latest execution status is retrying", SMMetadata.LatestExecution.Status.RETRYING, metadataToSave.deletion!!.latestExecution!!.status)
        }
    fun `test snapshots exceed max count`() =
        runBlocking {
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(11))
            mockDeleteSnapshotCall(response = AcknowledgedResponse(true))

            val metadata =
                randomSMMetadata(
                    deletionCurrentState = SMState.DELETION_CONDITION_MET,
                )
            val job =
                randomSMPolicy(
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

    fun `test snapshots exceed max age`() =
        runBlocking {
            val oldSnapshot = mockSnapshotInfo(name = "old_snapshot", startTime = now().minusSeconds(2 * 60).toEpochMilli())
            val newSnapshot = mockSnapshotInfo(name = "new_snapshot", startTime = now().toEpochMilli())
            mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(oldSnapshot, newSnapshot, newSnapshot)))
            mockDeleteSnapshotCall(response = AcknowledgedResponse(true))

            val metadata =
                randomSMMetadata(
                    deletionCurrentState = SMState.DELETION_CONDITION_MET,
                )
            val job =
                randomSMPolicy(
                    policyName = "daily-snapshot",
                    deletionMaxAge = TimeValue.timeValueMinutes(1),
                    deletionMinCount = 1, // to ensure at least one snapshot remains, as we have only two unique snapshot in mock response
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

    fun `test snapshots exceed max age but need to remain min count`() =
        runBlocking {
            val oldSnapshot = mockSnapshotInfo(name = "old_snapshot", startTime = now().minusSeconds(2 * 60).toEpochMilli())
            val newSnapshot = mockSnapshotInfo(name = "new_snapshot", startTime = now().toEpochMilli())
            mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(oldSnapshot, newSnapshot)))

            val metadata =
                randomSMMetadata(
                    deletionCurrentState = SMState.DELETION_CONDITION_MET,
                )
            val job =
                randomSMPolicy(
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

    fun `test snapshots min count check won't get negative deletion count`() =
        runBlocking {
            val oldSnapshot = mockSnapshotInfo(name = "old_snapshot", startTime = now().minusSeconds(2 * 60).toEpochMilli())
            mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(oldSnapshot)))

            val metadata =
                randomSMMetadata(
                    deletionCurrentState = SMState.DELETION_CONDITION_MET,
                )
            val job =
                randomSMPolicy(
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

    fun `test delete snapshot exception`() =
        runBlocking {
            val ex = Exception()
            mockGetSnapshotsCall(response = mockGetSnapshotResponse(11))
            mockDeleteSnapshotCall(exception = ex)

            val metadata =
                randomSMMetadata(
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

    fun `test get snapshots exception`() =
        runBlocking {
            val ex = Exception()
            mockGetSnapshotsCall(exception = ex)

            val metadata =
                randomSMMetadata(
                    deletionCurrentState = SMState.DELETION_CONDITION_MET,
                    deletionLatestExecution =
                    randomLatestExecution(
                        startTime = now().minusSeconds(10),
                    ),
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

    fun `test policy deletion is null`() =
        runBlocking {
            val metadata =
                randomSMMetadata(
                    deletionCurrentState = SMState.DELETION_CONDITION_MET,
                    startedDeletion = listOf(randomSnapshotName()),
                )
            val job =
                randomSMPolicy(
                    deletionNull = true,
                )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.DELETING.instance.execute(context)
            assertTrue("Execution result should be Fail.", result is SMResult.Fail)
            result as SMResult.Fail
            val metadataToSave = result.metadataToSave.build()
            assertNull("Deletion metadata should be null.", metadataToSave.deletion)
        }

    fun `test deletion with snapshot pattern combines policy and pattern snapshots`() =
        runBlocking {
            // Create policy snapshots and external pattern snapshots
            val policySnapshot1 = mockSnapshotInfo(name = "daily-policy-snapshot-1", startTime = now().minusSeconds(8 * 24 * 3600).toEpochMilli(), policyName = "daily-policy")
            val policySnapshot2 = mockSnapshotInfo(name = "daily-policy-snapshot-2", startTime = now().minusSeconds(2 * 24 * 3600).toEpochMilli(), policyName = "daily-policy")
            val patternSnapshot1 = mockSnapshotInfo(name = "external-backup-1", startTime = now().minusSeconds(10 * 24 * 3600).toEpochMilli(), policyName = "some-other-policy")
            val patternSnapshot2 = mockSnapshotInfo(name = "external-backup-2", startTime = now().minusSeconds(1 * 24 * 3600).toEpochMilli(), policyName = "some-other-policy")

            // Mock single call that returns all snapshots (both policy and pattern)
            // The actual implementation makes 2 calls but combines results, so we mock with all snapshots
            mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(policySnapshot1, policySnapshot2, patternSnapshot1, patternSnapshot2)))
            mockDeleteSnapshotCall(response = AcknowledgedResponse(true))

            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = null, // No creation workflow
                deletion = SMMetadata.WorkflowMetadata(
                    currentState = SMState.DELETION_CONDITION_MET,
                    trigger = SMMetadata.Trigger(time = now()),
                ),
            )
            val job = randomSMPolicy(
                policyName = "daily-policy",
                creationNull = true,
                deletionMaxAge = TimeValue.timeValueDays(7),
                deletionMinCount = 2,
                snapshotPattern = "external-*",
            )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.DELETING.instance.execute(context)
            assertTrue("Execution result should be Next.", result is SMResult.Next)
            result as SMResult.Next
            val metadataToSave = result.metadataToSave.build()

            // Should delete old snapshots from both policy and pattern that exceed max age
            val deletedSnapshots = metadataToSave.deletion?.started
            assertNotNull("Deletion started field should be initialized.", deletedSnapshots)
            assertTrue("Should delete old policy snapshot", deletedSnapshots?.contains("daily-policy-snapshot-1") == true)
            assertTrue("Should delete old pattern snapshot", deletedSnapshots?.contains("external-backup-1") == true)
        }

    fun `test deletion without pattern only processes policy snapshots`() =
        runBlocking {
            val policySnapshot1 = mockSnapshotInfo(name = "policy-snapshot-1", startTime = now().minusSeconds(8 * 24 * 3600).toEpochMilli(), policyName = "test-policy")
            val policySnapshot2 = mockSnapshotInfo(name = "policy-snapshot-2", startTime = now().minusSeconds(1 * 24 * 3600).toEpochMilli(), policyName = "test-policy")

            mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(policySnapshot1, policySnapshot2)))
            mockDeleteSnapshotCall(response = AcknowledgedResponse(true))

            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = null, // No creation workflow
                deletion = SMMetadata.WorkflowMetadata(
                    currentState = SMState.DELETION_CONDITION_MET,
                    trigger = SMMetadata.Trigger(time = now()),
                ),
            )
            val job = randomSMPolicy(
                policyName = "test-policy",
                creationNull = true,
                deletionMaxAge = TimeValue.timeValueDays(7),
                deletionMinCount = 1,
                snapshotPattern = null, // No pattern
            )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.DELETING.instance.execute(context)
            assertTrue("Execution result should be Next.", result is SMResult.Next)
            result as SMResult.Next
            val metadataToSave = result.metadataToSave.build()

            // Should only delete old policy snapshot
            val deletedSnapshots = metadataToSave.deletion?.started
            assertNotNull("Deletion started field should be initialized.", deletedSnapshots)
            assertEquals("Should delete 1 snapshot", 1, deletedSnapshots?.size)
            assertTrue("Should delete old policy snapshot", deletedSnapshots?.contains("policy-snapshot-1") == true)
        }

    fun `test deletion with pattern respects conditions across combined snapshots`() =
        runBlocking {
            val policySnapshot = mockSnapshotInfo(name = "policy-snapshot-1", startTime = now().minusSeconds(8 * 24 * 3600).toEpochMilli(), policyName = "test-policy")
            val patternSnapshot = mockSnapshotInfo(name = "external-backup-1", startTime = now().minusSeconds(10 * 24 * 3600).toEpochMilli(), policyName = "some-other-policy")

            // Mock single call that returns all snapshots (both policy and pattern)
            mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(policySnapshot, patternSnapshot)))

            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = null, // No creation workflow
                deletion = SMMetadata.WorkflowMetadata(
                    currentState = SMState.DELETION_CONDITION_MET,
                    trigger = SMMetadata.Trigger(time = now()),
                ),
            )
            val job = randomSMPolicy(
                policyName = "test-policy",
                creationNull = true,
                deletionMaxAge = TimeValue.timeValueDays(7),
                deletionMinCount = 2, // Require keeping 2 snapshots total
                snapshotPattern = "external-*",
            )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.DELETING.instance.execute(context)
            assertTrue("Execution result should be Next.", result is SMResult.Next)
            result as SMResult.Next
            val metadataToSave = result.metadataToSave.build()

            // Should not delete any snapshots to maintain min count
            assertNull("Should not delete snapshots due to min count constraint", metadataToSave.deletion?.started)
        }

    fun `test deletion-only policy works without creation workflow`() =
        runBlocking {
            val oldSnapshot = mockSnapshotInfo(name = "old_snapshot", startTime = now().minusSeconds(8 * 24 * 3600).toEpochMilli(), policyName = "deletion-only-policy")
            val newSnapshot = mockSnapshotInfo(name = "new_snapshot", startTime = now().toEpochMilli(), policyName = "deletion-only-policy")
            mockGetSnapshotsCall(response = mockGetSnapshotsResponse(listOf(oldSnapshot, newSnapshot)))
            mockDeleteSnapshotCall(response = AcknowledgedResponse(true))

            val metadata = SMMetadata(
                policySeqNo = 1L,
                policyPrimaryTerm = 1L,
                creation = null, // No creation workflow
                deletion = SMMetadata.WorkflowMetadata(
                    currentState = SMState.DELETION_CONDITION_MET,
                    trigger = SMMetadata.Trigger(time = now()),
                ),
            )
            val job = randomSMPolicy(
                policyName = "deletion-only-policy",
                creationNull = true, // No creation
                deletionMaxAge = TimeValue.timeValueDays(7),
                deletionMinCount = 1,
            )
            val context = SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)

            val result = SMState.DELETING.instance.execute(context)
            assertTrue("Execution result should be Next.", result is SMResult.Next)
            result as SMResult.Next
            val metadataToSave = result.metadataToSave.build()

            // Should work fine without creation workflow
            assertNull("Creation workflow should be null", metadataToSave.creation)
            assertNotNull("Deletion workflow should exist", metadataToSave.deletion)
            val deletedSnapshots = metadataToSave.deletion?.started
            assertNotNull("Should delete old snapshot", deletedSnapshots)
            assertTrue("Should delete old snapshot", deletedSnapshots?.contains("old_snapshot") == true)
        }
}
