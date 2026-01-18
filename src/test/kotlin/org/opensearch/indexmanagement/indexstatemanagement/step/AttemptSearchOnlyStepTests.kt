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
import org.mockito.ArgumentMatchers.anyBoolean
import org.mockito.ArgumentMatchers.anyString
import org.opensearch.action.admin.indices.scale.searchonly.ScaleIndexRequestBuilder
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.action.ActionFuture
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.step.searchonly.AttemptSearchOnlyStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient

class AttemptSearchOnlyStepTests : OpenSearchTestCase() {
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = mock()
    private lateinit var metricsRegistry: MetricsRegistry

    @Before
    fun setup() {
        metricsRegistry = mock()
        whenever(metricsRegistry.createCounter(anyString(), anyString(), anyString())).thenAnswer {
            mock<Counter>()
        }
        IndexManagementActionsMetrics.instance.initialize(metricsRegistry)
        ManagedIndexRunner.registerIndexManagementActionMetrics(IndexManagementActionsMetrics.instance)
    }

    fun `test search_only step sets step status to completed when successful`() {
        val acknowledgedResponse = AcknowledgedResponse(true)
        val clusterService = getClusterService(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(acknowledgedResponse, null)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptSearchOnlyStep = AttemptSearchOnlyStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSearchOnlyStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptSearchOnlyStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test search_only step sets step status to failed when not acknowledged`() {
        val acknowledgedResponse = AcknowledgedResponse(false)
        val clusterService = getClusterService(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(acknowledgedResponse, null)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptSearchOnlyStep = AttemptSearchOnlyStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSearchOnlyStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptSearchOnlyStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test search_only step sets step status to completed when already in search-only mode`() {
        val clusterService = getClusterService(true)
        val client = getClient(getAdminClient(getIndicesAdminClient(null, null)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptSearchOnlyStep = AttemptSearchOnlyStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSearchOnlyStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptSearchOnlyStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertTrue("Message should indicate already in search-only mode", updatedManagedIndexMetaData.info!!["message"].toString().contains("already"))
        }
    }

    fun `test search_only step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val clusterService = getClusterService(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptSearchOnlyStep = AttemptSearchOnlyStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSearchOnlyStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptSearchOnlyStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test search_only step sets step status to condition not met when snapshot in progress error thrown`() {
        val exception = SnapshotInProgressException("example")
        val clusterService = getClusterService(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptSearchOnlyStep = AttemptSearchOnlyStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSearchOnlyStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptSearchOnlyStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test search_only step remote transport snapshot in progress exception`() {
        val exception = RemoteTransportException("rte", SnapshotInProgressException("nested"))
        val clusterService = getClusterService(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptSearchOnlyStep = AttemptSearchOnlyStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSearchOnlyStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptSearchOnlyStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test search_only step remote transport exception`() {
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val clusterService = getClusterService(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptSearchOnlyStep = AttemptSearchOnlyStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSearchOnlyStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptSearchOnlyStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test search_only step sets step status to condition not met when rejected execution exception thrown`() {
        val exception = OpenSearchRejectedExecutionException("backpressure")
        val clusterService = getClusterService(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptSearchOnlyStep = AttemptSearchOnlyStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSearchOnlyStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptSearchOnlyStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test search_only step sets step status to condition not met when timeout exception thrown`() {
        val exception = RuntimeException("operation timed out waiting for remote store sync")
        val clusterService = getClusterService(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptSearchOnlyStep = AttemptSearchOnlyStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSearchOnlyStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptSearchOnlyStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    fun `test search_only step remote transport rejected execution exception`() {
        val exception = RemoteTransportException("rte", OpenSearchRejectedExecutionException("backpressure"))
        val clusterService = getClusterService(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val attemptSearchOnlyStep = AttemptSearchOnlyStep()
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptSearchOnlyStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptSearchOnlyStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    private fun getClusterService(isSearchOnly: Boolean): ClusterService {
        val indexSettings = Settings.builder()
            .put("index.blocks.search_only", isSearchOnly)
            .build()
        val indexMetadata: IndexMetadata = mock {
            on { settings } doReturn indexSettings
        }
        val metadata: Metadata = mock {
            on { index("test") } doReturn indexMetadata
        }
        val clusterState: ClusterState = mock {
            on { this.metadata } doReturn metadata
        }
        return mock {
            on { state() } doReturn clusterState
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }

    private fun getIndicesAdminClient(acknowledgedResponse: AcknowledgedResponse?, exception: Exception?): IndicesAdminClient {
        val requestBuilder: ScaleIndexRequestBuilder = mock()

        doAnswer { invocationOnMock ->
            val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(0)
            if (acknowledgedResponse != null) {
                listener.onResponse(acknowledgedResponse)
            } else if (exception != null) {
                listener.onFailure(exception)
            }
            mock<ActionFuture<AcknowledgedResponse>>()
        }.whenever(requestBuilder).execute(any())

        return mock {
            on { prepareScaleSearchOnly(anyString(), anyBoolean()) } doReturn requestBuilder
        }
    }
}
