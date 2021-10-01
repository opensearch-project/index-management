/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.ClusterAdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import org.opensearch.indexmanagement.indexstatemanagement.randomSnapshotActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.SNAPSHOT_DENY_LIST
import org.opensearch.indexmanagement.indexstatemanagement.step.snapshot.AttemptSnapshotStep
import org.opensearch.ingest.TestTemplateService.MockTemplateScript
import org.opensearch.rest.RestStatus
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.snapshots.ConcurrentSnapshotExecutionException
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException

class AttemptSnapshotStepTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val config = randomSnapshotActionConfig("repo", "snapshot-name")
    private val metadata = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, ActionMetaData(AttemptSnapshotStep.name, 1, 0, false, 0, null, ActionProperties(snapshotName = "snapshot-name")), null, null, null)

    @Before
    fun settings() {
        whenever(clusterService.clusterSettings).doReturn(ClusterSettings(Settings.EMPTY, setOf(SNAPSHOT_DENY_LIST)))
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(MockTemplateScript.Factory("snapshot-name"))
    }

    fun `test snapshot response when block`() {
        val response: CreateSnapshotResponse = mock()
        val client = getClient(getAdminClient(getClusterAdminClient(response, null)))

        whenever(response.status()).doReturn(RestStatus.ACCEPTED)
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, scriptService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }

        whenever(response.status()).doReturn(RestStatus.OK)
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, scriptService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }

        whenever(response.status()).doReturn(RestStatus.INTERNAL_SERVER_ERROR)
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, scriptService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test snapshot exception`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, scriptService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "example", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test snapshot concurrent snapshot exception`() {
        val exception = ConcurrentSnapshotExecutionException("repo", "other-snapshot", "concurrent snapshot in progress")
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, scriptService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get failed concurrent message", AttemptSnapshotStep.getFailedConcurrentSnapshotMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot remote transport concurrent exception`() {
        val exception = RemoteTransportException("rte", ConcurrentSnapshotExecutionException("repo", "other-snapshot", "concurrent snapshot in progress"))
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, scriptService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get failed concurrent message", AttemptSnapshotStep.getFailedConcurrentSnapshotMessage("test"), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test snapshot remote transport normal exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("some error"))
        val client = getClient(getAdminClient(getClusterAdminClient(null, exception)))
        runBlocking {
            val step = AttemptSnapshotStep(clusterService, scriptService, client, config, metadata)
            step.execute()
            val updatedManagedIndexMetaData = step.getUpdatedManagedIndexMetaData(metadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "some error", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(clusterAdminClient: ClusterAdminClient): AdminClient = mock { on { cluster() } doReturn clusterAdminClient }
    private fun getClusterAdminClient(createSnapshotRequest: CreateSnapshotResponse?, exception: Exception?): ClusterAdminClient {
        assertTrue("Must provide one and only one response or exception", (createSnapshotRequest != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<CreateSnapshotResponse>>(1)
                if (createSnapshotRequest != null) listener.onResponse(createSnapshotRequest)
                else listener.onFailure(exception)
            }.whenever(this.mock).createSnapshot(any(), any())
        }
    }
}
