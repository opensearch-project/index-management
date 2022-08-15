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
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.close.CloseIndexResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.step.close.AttemptCloseStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException
import kotlin.IllegalArgumentException

class AttemptCloseStepTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)

    fun `test close step sets step status to completed when successful`() {
        val closeIndexResponse = CloseIndexResponse(true, true, listOf())
        val client = getClient(getAdminClient(getIndicesAdminClient(closeIndexResponse, null)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptCloseStep = AttemptCloseStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptCloseStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptCloseStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test close step sets step status to failed when not acknowledged`() {
        val closeIndexResponse = CloseIndexResponse(false, false, listOf())
        val client = getClient(getAdminClient(getIndicesAdminClient(closeIndexResponse, null)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptCloseStep = AttemptCloseStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptCloseStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptCloseStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test close step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptCloseStep = AttemptCloseStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptCloseStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptCloseStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test close step sets step status to condition not met when snapshot in progress error thrown`() {
        val exception = SnapshotInProgressException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptCloseStep = AttemptCloseStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptCloseStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptCloseStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test close step remote transport snapshot in progress exception`() {
        val exception = RemoteTransportException("rte", SnapshotInProgressException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptCloseStep = AttemptCloseStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptCloseStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptCloseStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test close step remote transport exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptCloseStep = AttemptCloseStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptCloseStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptCloseStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(closeIndexResponse: CloseIndexResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (closeIndexResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<CloseIndexResponse>>(1)
                if (closeIndexResponse != null) listener.onResponse(closeIndexResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).close(any(), any())
        }
    }
}
