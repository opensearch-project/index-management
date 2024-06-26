/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.eq
import org.opensearch.action.admin.indices.rollover.RolloverResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.RolloverActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry
import org.opensearch.telemetry.metrics.tags.Tags
import org.opensearch.test.OpenSearchTestCase

class AttemptRolloverStepTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)
    private val oldIndexName = "old_index"
    private val newIndexName = "new_index"
    val alias = "alias"
    private lateinit var metricsRegistry: MetricsRegistry
    private lateinit var rolloverActionMetrics: RolloverActionMetrics

    @Before
    fun setup() {
        // Setup metrics
        metricsRegistry = mock()
        whenever(metricsRegistry.createCounter(anyString(), anyString(), anyString())).thenAnswer {
            mock<Counter>()
        }
        IndexManagementActionsMetrics.instance.initialize(metricsRegistry)
        ManagedIndexRunner.registerIndexManagementActionMetrics(IndexManagementActionsMetrics.instance)
        rolloverActionMetrics = IndexManagementActionsMetrics.instance.getActionMetrics(IndexManagementActionsMetrics.ROLLOVER) as RolloverActionMetrics

        // mock rollover target
        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: IndexMetadata = mock()
        val settings =
            Settings.builder()
                .put(ManagedIndexSettings.ROLLOVER_ALIAS.key, alias)
                .put(ManagedIndexSettings.ROLLOVER_SKIP.key, false)
                .build()
        whenever(clusterService.state()).thenReturn(clusterState)
        whenever(clusterState.metadata()).thenReturn(metadata)
        whenever(clusterState.metadata).thenReturn(metadata)
        whenever(metadata.index(oldIndexName)).thenReturn(indexMetadata)
        whenever(metadata.indicesLookup).thenReturn(sortedMapOf())
        whenever(indexMetadata.settings).thenReturn(settings)

        // mock rolloverInfos
        whenever(metadata.index(oldIndexName).rolloverInfos).thenReturn(mapOf(alias to mock()))
    }

    fun `test copy alias in rollover step is not acknowledged`() {
        val rolloverResponse = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, true, true)
        val aliasResponse = AcknowledgedResponse(false)
        // val exception = Exception("test exception")
        val client = getClient(getAdminClient(getIndicesAdminClient(rolloverResponse, aliasResponse, null, null)))

        runBlocking {
            val rolloverAction = RolloverAction(null, null, null, null, true, 0)
            val managedIndexMetaData =
                ManagedIndexMetaData(
                    oldIndexName, "indexUuid", "policy_id",
                    null, null, null,
                    null, null, null,
                    null, null, null,
                    null, null, rolledOverIndexName = newIndexName,
                )
            val attemptRolloverStep = AttemptRolloverStep(rolloverAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptRolloverStep.preExecute(logger, context).execute(ManagedIndexRunner.indexManagementActionMetrics)
            val updatedManagedIndexMetaData = attemptRolloverStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("message info is not matched", AttemptRolloverStep.getCopyAliasNotAckMessage(oldIndexName, newIndexName), updatedManagedIndexMetaData.info?.get("message"))
            verify(rolloverActionMetrics.failures).add(eq(1.0), any<Tags>())
        }
    }

    fun `test copy alias in rollover step failed`() {
        val rolloverResponse = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, true, true)
        // val aliasResponse = AcknowledgedResponse(true)
        val exception = Exception("test exception")
        val client = getClient(getAdminClient(getIndicesAdminClient(rolloverResponse, null, null, exception)))

        runBlocking {
            val rolloverAction = RolloverAction(null, null, null, null, true, 0)
            val managedIndexMetaData =
                ManagedIndexMetaData(
                    oldIndexName, "indexUuid", "policy_id",
                    null, null, null,
                    null, null, null,
                    null, null, null,
                    null, null, rolledOverIndexName = newIndexName,
                )
            val attemptRolloverStep = AttemptRolloverStep(rolloverAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptRolloverStep.preExecute(logger, context).execute(ManagedIndexRunner.indexManagementActionMetrics)
            val updatedManagedIndexMetaData = attemptRolloverStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("message info is not matched", AttemptRolloverStep.getFailedCopyAliasMessage(oldIndexName, newIndexName), updatedManagedIndexMetaData.info?.get("message"))
            verify(rolloverActionMetrics.failures).add(eq(1.0), any<Tags>())
        }
    }

    fun `test copy alias in rollover step failed with index not found exception`() {
        val rolloverResponse = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, true, true)
        // val aliasResponse = AcknowledgedResponse(true)
        val exception = IndexNotFoundException("test exception")
        val client = getClient(getAdminClient(getIndicesAdminClient(rolloverResponse, null, null, exception)))

        runBlocking {
            val rolloverAction = RolloverAction(null, null, null, null, true, 0)
            val managedIndexMetaData =
                ManagedIndexMetaData(
                    oldIndexName, "indexUuid", "policy_id",
                    null, null, null,
                    null, null, null,
                    null, null, null,
                    null, null, rolledOverIndexName = newIndexName,
                )
            val attemptRolloverStep = AttemptRolloverStep(rolloverAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptRolloverStep.preExecute(logger, context).execute(ManagedIndexRunner.indexManagementActionMetrics)
            val updatedManagedIndexMetaData = attemptRolloverStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("message info is not matched", AttemptRolloverStep.getCopyAliasIndexNotFoundMessage(newIndexName), updatedManagedIndexMetaData.info?.get("message"))
            verify(rolloverActionMetrics.failures).add(eq(1.0), any<Tags>())
        }
    }

    fun `test copy alias in rollover step but no rollodOverIndexName`() {
        val rolloverResponse = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, true, true)
        val aliasResponse = AcknowledgedResponse(true)
        // val exception = IndexNotFoundException("test exception")
        val client = getClient(getAdminClient(getIndicesAdminClient(rolloverResponse, aliasResponse, null, null)))

        runBlocking {
            val rolloverAction = RolloverAction(null, null, null, null, true, 0)
            val managedIndexMetaData =
                ManagedIndexMetaData(
                    oldIndexName, "indexUuid", "policy_id",
                    null, null, null,
                    null, null, null,
                    null, null, null,
                    null, null, rolledOverIndexName = null,
                )
            val attemptRolloverStep = AttemptRolloverStep(rolloverAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptRolloverStep.preExecute(logger, context).execute(ManagedIndexRunner.indexManagementActionMetrics)
            val updatedManagedIndexMetaData = attemptRolloverStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("message info is not matched", AttemptRolloverStep.getCopyAliasRolledOverIndexNotFoundMessage(oldIndexName), updatedManagedIndexMetaData.info?.get("message"))
            verify(rolloverActionMetrics.successes).add(eq(1.0), any<Tags>())
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }

    private fun getIndicesAdminClient(
        rolloverResponse: RolloverResponse?,
        aliasResponse: AcknowledgedResponse?,
        rolloverException: Exception?,
        aliasException: Exception?,
    ): IndicesAdminClient {
        assertTrue(
            "Must provide one and only one response or exception",
            (rolloverResponse != null).xor(rolloverException != null),
        )
        assertTrue(
            "Must provide one and only one response or exception",
            (aliasResponse != null).xor(aliasException != null),
        )
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
                if (rolloverResponse != null) {
                    listener.onResponse(rolloverResponse)
                } else {
                    listener.onFailure(rolloverException)
                }
            }.whenever(this.mock).rolloverIndex(any(), any())

            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
                if (aliasResponse != null) {
                    listener.onResponse(aliasResponse)
                } else {
                    listener.onFailure(aliasException)
                }
            }.whenever(this.mock).aliases(any(), any())
        }
    }
}
