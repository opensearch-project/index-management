/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.ClusterAdminClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.indexstatemanagement.step.restore.WaitForRestoreStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException

class WaitForRestoreStepTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)

    fun `test restore status states`() {
        val restoreEntry: RestoreInProgress.Entry = mock()
        whenever(restoreEntry.indices()).doReturn(listOf("test"))
        val restoreInProgress = RestoreInProgress.Builder()
            .add(restoreEntry)
            .build()
        val clusterState: ClusterState = mock()
        whenever(clusterState.custom<RestoreInProgress>(RestoreInProgress.TYPE)).thenReturn(restoreInProgress)
        whenever(clusterService.state()).thenReturn(clusterState)
        val response: SnapshotsStatusResponse = mock()
        val client = getClient(getAdminClient(getClusterAdminClient(response, null)))

        val metadata = ManagedIndexMetaData(
            "test",
            "indexUuid",
            "policy_id",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            ActionMetaData(
                WaitForRestoreStep.name,
                1,
                0,
                false,
                0,
                null,
                ActionProperties(snapshotName = "snapshot-name"),
            ),
            null,
            null,
            null,
        )
        runBlocking {
            val step = WaitForRestoreStep()
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals(
                "Step status is not CONDITION_NOT_MET",
                Step.StepStatus.CONDITION_NOT_MET,
                updatedMetaData.stepMetaData?.stepStatus,
            )
            assertEquals(
                "Did not get restore in progress message",
                WaitForRestoreStep.getPendingMessage("test"),
                updatedMetaData.info!!["message"],
            )
        }

        // Test SUCCESS state
        whenever(restoreEntry.indices()).doReturn(listOf("other-test"))
        runBlocking {
            val step = WaitForRestoreStep()
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals(
                "Step status is not COMPLETED",
                Step.StepStatus.COMPLETED,
                updatedMetaData.stepMetaData?.stepStatus,
            )
            assertEquals(
                "Did not get restore completed message",
                WaitForRestoreStep.getSuccessMessage("test"),
                updatedMetaData.info!!["message"],
            )
        }
    }

    fun `test restore not in response list`() {
        val client = getClient(getAdminClient(getClusterAdminClient(null, IllegalArgumentException("not used"))))

        val metadata = ManagedIndexMetaData(
            "test",
            "indexUuid",
            "policy_id",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            ActionMetaData(
                WaitForRestoreStep.name,
                1,
                0,
                false,
                0,
                null,
                ActionProperties(snapshotName = "snapshot-name"),
            ),
            null,
            null,
            null,
        )
        val restoreEntry: RestoreInProgress.Entry = mock()
        whenever(restoreEntry.indices()).doReturn(listOf("not-test"))
        val restoreInProgress = RestoreInProgress.Builder()
            .add(restoreEntry)
            .build()
        val clusterState: ClusterState = mock()
        whenever(clusterState.custom<RestoreInProgress>(RestoreInProgress.TYPE)).thenReturn(restoreInProgress)
        whenever(clusterService.state()).thenReturn(clusterState)

        runBlocking {
            val step = WaitForRestoreStep()
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals(
                "Step status is not COMPLETED",
                Step.StepStatus.COMPLETED,
                updatedMetaData.stepMetaData?.stepStatus,
            )
            assertEquals(
                "Did not get restore completed message",
                WaitForRestoreStep.getSuccessMessage("test"),
                updatedMetaData.info!!["message"],
            )
        }
    }

    fun `test restore exception`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        whenever(clusterService.state()).thenThrow(exception)
        val metadata = ManagedIndexMetaData(
            "test",
            "indexUuid",
            "policy_id",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            ActionMetaData(
                WaitForRestoreStep.name,
                1,
                0,
                false,
                0,
                null,
                ActionProperties(snapshotName = "snapshot-name"),
            ),
            null,
            null,
            null,
        )

        runBlocking {
            val step = WaitForRestoreStep()
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals(
                "Step status is not FAILED",
                Step.StepStatus.FAILED,
                updatedMetaData.stepMetaData?.stepStatus,
            )
            assertEquals(
                "Did not get cause from exception",
                "example",
                updatedMetaData.info!!["cause"],
            )
        }
    }

    fun `test restore remote transport exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        whenever(clusterService.state()).thenThrow(exception)
        val metadata = ManagedIndexMetaData(
            "test",
            "indexUuid",
            "policy_id",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            ActionMetaData(
                WaitForRestoreStep.name,
                1,
                0,
                false,
                0,
                null,
                ActionProperties(snapshotName = "snapshot-name"),
            ),
            null,
            null,
            null,
        )

        runBlocking {
            val step = WaitForRestoreStep()
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals(
                "Step status is not FAILED",
                Step.StepStatus.FAILED,
                updatedMetaData.stepMetaData?.stepStatus,
            )
            assertEquals(
                "Did not get cause from nested exception",
                "nested",
                updatedMetaData.info!!["cause"],
            )
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(clusterAdminClient: ClusterAdminClient): AdminClient = mock { on { cluster() } doReturn clusterAdminClient }
    private fun getClusterAdminClient(response: SnapshotsStatusResponse?, exception: Exception?): ClusterAdminClient {
        assertTrue("Must provide one and only one response or exception", (response != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<SnapshotsStatusResponse>>(1)
                if (response != null) {
                    listener.onResponse(response)
                } else {
                    listener.onFailure(exception)
                }
            }.whenever(this.mock).snapshotsStatus(any(), any())
        }
    }
}
