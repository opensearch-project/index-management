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
import org.junit.Before
import org.mockito.ArgumentMatchers.anyString
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.action.IndexPriorityAction
import org.opensearch.indexmanagement.indexstatemanagement.step.indexpriority.AttemptSetIndexPriorityStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.SetIndexPriorityActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import java.time.Instant

class AttemptSetIndexPriorityStepTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = mock()
    private lateinit var metricsRegistry: MetricsRegistry
    private lateinit var setIndexPriorityActionMetrics: SetIndexPriorityActionMetrics

    @Before
    fun setup() {
        metricsRegistry = mock()
        whenever(metricsRegistry.createCounter(anyString(), anyString(), anyString())).thenAnswer {
            mock<Counter>()
        }
        IndexManagementActionsMetrics.instance.initialize(metricsRegistry)
        ManagedIndexRunner.registerIndexManagementActionMetrics(IndexManagementActionsMetrics.instance)
        setIndexPriorityActionMetrics = IndexManagementActionsMetrics.instance.getActionMetrics(IndexManagementActionsMetrics.SET_INDEX_PRIORITY) as SetIndexPriorityActionMetrics
    }

    fun `test set priority step sets step status to completed when successful`() {
        val acknowledgedResponse = AcknowledgedResponse(true)
        val client = getClient(getAdminClient(getIndicesAdminClient(acknowledgedResponse, null)))

        runBlocking {
            val indexPriorityAction = IndexPriorityAction(50, 0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData("set_index_priority", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null)
            val attemptSetPriorityStep = AttemptSetIndexPriorityStep(indexPriorityAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSetPriorityStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptSetPriorityStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptSetPriorityStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test set priority step sets step status to failed when not acknowledged`() {
        val acknowledgedResponse = AcknowledgedResponse(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(acknowledgedResponse, null)))

        runBlocking {
            val indexPriorityAction = IndexPriorityAction(50, 0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData("set_index_priority", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null)
            val attemptSetPriorityStep = AttemptSetIndexPriorityStep(indexPriorityAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSetPriorityStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptSetPriorityStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptSetPriorityStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test set priority step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val indexPriorityAction = IndexPriorityAction(50, 0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData("set_index_priority", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null)
            val attemptSetPriorityStep = AttemptSetIndexPriorityStep(indexPriorityAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSetPriorityStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptSetPriorityStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptSetPriorityStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            logger.info(updatedManagedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test set priority step sets step status to failed when remote transport error thrown`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val indexPriorityAction = IndexPriorityAction(50, 0)
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData("set_index_priority", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null)
            val attemptSetPriorityStep = AttemptSetIndexPriorityStep(indexPriorityAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSetPriorityStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptSetPriorityStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptSetPriorityStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            logger.info(updatedManagedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }

    private fun getIndicesAdminClient(acknowledgedResponse: AcknowledgedResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (acknowledgedResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
                if (acknowledgedResponse != null) {
                    listener.onResponse(acknowledgedResponse)
                } else {
                    listener.onFailure(exception)
                }
            }.whenever(this.mock).updateSettings(any(), any())
        }
    }
}
