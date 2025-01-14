/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.ClusterAdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction
import org.opensearch.indexmanagement.indexstatemanagement.step.snapshot.WaitForSnapshotStep
import org.opensearch.indexmanagement.snapshotmanagement.mockInProgressSnapshotInfo
import org.opensearch.indexmanagement.snapshotmanagement.mockSnapshotInfo
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.snapshots.SnapshotState
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException

class WaitForSnapshotStepTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)
    val snapshot = "snapshot-name"

    fun `test snapshot missing snapshot name in action properties`() {
        val exception = IllegalArgumentException("not used")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val emptyActionProperties = ActionProperties()
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, emptyActionProperties), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", WaitForSnapshotStep.getFailedActionPropertiesMessage("test", emptyActionProperties), updatedManagedIndexMetaData.info!!["message"])
        }

        runBlocking {
            val nullActionProperties = null
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, nullActionProperties), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", WaitForSnapshotStep.getFailedActionPropertiesMessage("test", nullActionProperties), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot status states`() {
        val snapshotInfo: SnapshotInfo = mockInProgressSnapshotInfo(snapshot)
        val response: GetSnapshotsResponse = mock()
        whenever(response.snapshots).doReturn(listOf(snapshotInfo))
        val client = getClient(getAdminClient(getClusterAdminClient(response, null)))
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot in progress message", WaitForSnapshotStep.getSnapshotInProgressMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        val snapshotInfo2: SnapshotInfo = mockSnapshotInfo(snapshot, SnapshotState.SUCCESS)
        whenever(response.snapshots).doReturn(listOf(snapshotInfo2))
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot completed message", WaitForSnapshotStep.getSuccessMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        val snapshotInfo3: SnapshotInfo = mockSnapshotInfo(snapshot, SnapshotState.FAILED)
        whenever(response.snapshots).doReturn(listOf(snapshotInfo3))
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot failed message", WaitForSnapshotStep.getFailedExistsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        val snapshotInfo4: SnapshotInfo = mockSnapshotInfo(snapshot, SnapshotState.PARTIAL)
        whenever(response.snapshots).doReturn(listOf(snapshotInfo4))
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot failed message", WaitForSnapshotStep.getFailedExistsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }

        val snapshotInfo5: SnapshotInfo = mockSnapshotInfo(snapshot, SnapshotState.INCOMPATIBLE)
        whenever(response.snapshots).doReturn(listOf(snapshotInfo5))
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot failed message", WaitForSnapshotStep.getFailedExistsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot not in response list`() {
        val snapshotInfo: SnapshotInfo = mockSnapshotInfo("snapshot-different-name")
        val response: GetSnapshotsResponse = mock()
        whenever(response.snapshots).doReturn(listOf(snapshotInfo))
        val client = getClient(getAdminClient(getClusterAdminClient(response, null)))

        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get snapshot failed message", WaitForSnapshotStep.getFailedExistsMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot exception`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "example", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test snapshot remote transport exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val snapshotAction = SnapshotAction("repo", snapshot, 0)
            val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData(WaitForSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)
            val step = WaitForSnapshotStep(snapshotAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(clusterAdminClient: ClusterAdminClient): AdminClient = mock { on { cluster() } doReturn clusterAdminClient }

    private fun getClusterAdminClient(getSnapshotsResponse: GetSnapshotsResponse?, exception: Exception?): ClusterAdminClient {
        assertTrue("Must provide one and only one response or exception", (getSnapshotsResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<GetSnapshotsResponse>>(1)
                if (getSnapshotsResponse != null) {
                    listener.onResponse(getSnapshotsResponse)
                } else {
                    listener.onFailure(exception)
                }
            }.whenever(this.mock).getSnapshots(any(), any())
        }
    }
}
