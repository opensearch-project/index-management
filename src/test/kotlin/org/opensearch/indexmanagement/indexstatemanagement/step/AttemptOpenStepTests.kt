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
import org.opensearch.action.admin.indices.open.OpenIndexResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.step.open.AttemptOpenStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException

class AttemptOpenStepTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)

    fun `test open step sets step status to failed when not acknowledged`() {
        val openIndexResponse = OpenIndexResponse(false, false)
        val client = getClient(getAdminClient(getIndicesAdminClient(openIndexResponse, null)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptOpenStep = AttemptOpenStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptOpenStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptOpenStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test open step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptOpenStep = AttemptOpenStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptOpenStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptOpenStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test open step remote transport exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptOpenStep = AttemptOpenStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptOpenStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptOpenStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(openIndexResponse: OpenIndexResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (openIndexResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<OpenIndexResponse>>(1)
                if (openIndexResponse != null) listener.onResponse(openIndexResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).open(any(), any())
        }
    }
}
