/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.mockito.Mockito.doAnswer
import org.opensearch.action.ActionListener
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.step.delete.AttemptDeleteStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.test.OpenSearchTestCase

class AttemptDeleteStepTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)

    fun `test delete step sets step status to completed when successful`() {
        val acknowledgedResponse = AcknowledgedResponse(true)
        val client = getClient(getAdminClient(getIndicesAdminClient(acknowledgedResponse, null)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptDeleteStep = AttemptDeleteStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test delete step sets step status to failed when not acknowledged`() {
        val acknowledgedResponse = AcknowledgedResponse(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(acknowledgedResponse, null)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptDeleteStep = AttemptDeleteStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test delete step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptDeleteStep = AttemptDeleteStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            logger.info(updatedManagedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test delete step sets step status to condition not met when snapshot in progress error thrown`() {
        val exception = SnapshotInProgressException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptDeleteStep = AttemptDeleteStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(acknowledgedResponse: AcknowledgedResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (acknowledgedResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
                if (acknowledgedResponse != null) listener.onResponse(acknowledgedResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).delete(any(), any())
        }
    }
}
