/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.eq
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.admin.indices.rollover.RolloverResponse
import org.opensearch.action.admin.indices.stats.CommonStats
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.IndexAbstraction
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.RolloverActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry
import org.opensearch.telemetry.metrics.tags.Tags
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import java.time.Instant

class AttemptRolloverStepTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = mock()
    private val oldIndexName = "old_index"
    private val newIndexName = "new_index"
    val alias = "alias"
    val additionalAlias = "additional_alias"
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

        // Mock the aliases: rollover alias + an additional alias to copy
        whenever(indexMetadata.aliases).thenReturn(
            mapOf(
                alias to AliasMetadata.builder(alias).build(),
                additionalAlias to AliasMetadata.builder(additionalAlias).build(),
            ),
        )
    }

    fun `test copy alias in rollover step is not acknowledged`() {
        val rolloverResponse = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, true, true)
        val aliasResponse = AcknowledgedResponse(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(rolloverResponse, aliasResponse, null, null)))

        runBlocking {
            val rolloverAction = RolloverAction(null, null, null, null, true, false, index = 0)
            val managedIndexMetaData =
                ManagedIndexMetaData(
                    oldIndexName, "indexUuid", "policy_id",
                    null, null, null,
                    null, null, null,
                    null, ActionMetaData("rollover", Instant.now().toEpochMilli(), 0, false, 1, null, null), null,
                    null, null, rolledOverIndexName = newIndexName,
                )
            val attemptRolloverStep = AttemptRolloverStep(rolloverAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptRolloverStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptRolloverStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptRolloverStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("message info is not matched", AttemptRolloverStep.getCopyAliasNotAckMessage(oldIndexName, newIndexName), updatedManagedIndexMetaData.info?.get("message"))
            verify(rolloverActionMetrics.failures).add(eq(1.0), any<Tags>())
        }
    }

    fun `test copy alias in rollover step failed`() {
        val rolloverResponse = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, true, true)
        val exception = Exception("test exception")
        val client = getClient(getAdminClient(getIndicesAdminClient(rolloverResponse, null, null, exception)))

        runBlocking {
            val rolloverAction = RolloverAction(null, null, null, null, true, false, index = 0)
            val managedIndexMetaData =
                ManagedIndexMetaData(
                    oldIndexName, "indexUuid", "policy_id",
                    null, null, null,
                    null, null, null,
                    null, ActionMetaData("rollover", Instant.now().toEpochMilli(), 0, false, 1, null, null), null,
                    null, null, rolledOverIndexName = newIndexName,
                )
            val attemptRolloverStep = AttemptRolloverStep(rolloverAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptRolloverStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptRolloverStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptRolloverStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("message info is not matched", AttemptRolloverStep.getFailedCopyAliasMessage(oldIndexName, newIndexName), updatedManagedIndexMetaData.info?.get("message"))
            verify(rolloverActionMetrics.failures).add(eq(1.0), any<Tags>())
        }
    }

    fun `test copy alias in rollover step failed with index not found exception`() {
        val rolloverResponse = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, true, true)
        val exception = IndexNotFoundException("test exception")
        val client = getClient(getAdminClient(getIndicesAdminClient(rolloverResponse, null, null, exception)))

        runBlocking {
            val rolloverAction = RolloverAction(null, null, null, null, true, false, index = 0)
            val managedIndexMetaData =
                ManagedIndexMetaData(
                    oldIndexName, "indexUuid", "policy_id",
                    null, null, null,
                    null, null, null,
                    null, ActionMetaData("rollover", Instant.now().toEpochMilli(), 0, false, 1, null, null), null,
                    null, null, rolledOverIndexName = newIndexName,
                )
            val attemptRolloverStep = AttemptRolloverStep(rolloverAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptRolloverStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptRolloverStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptRolloverStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("message info is not matched", AttemptRolloverStep.getCopyAliasIndexNotFoundMessage(newIndexName), updatedManagedIndexMetaData.info?.get("message"))
            verify(rolloverActionMetrics.failures).add(eq(1.0), any<Tags>())
        }
    }

    fun `test copy alias in rollover step but no rollodOverIndexName`() {
        val rolloverResponse = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, true, true)
        val aliasResponse = AcknowledgedResponse(true)
        val client = getClient(getAdminClient(getIndicesAdminClient(rolloverResponse, aliasResponse, null, null)))

        runBlocking {
            val rolloverAction = RolloverAction(null, null, null, null, true, false, index = 0)
            val managedIndexMetaData =
                ManagedIndexMetaData(
                    oldIndexName, "indexUuid", "policy_id",
                    null, null, null,
                    null, null, null,
                    null, ActionMetaData("rollover", Instant.now().toEpochMilli(), 0, false, 1, null, null), null,
                    null, null, rolledOverIndexName = null,
                )
            val attemptRolloverStep = AttemptRolloverStep(rolloverAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptRolloverStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptRolloverStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptRolloverStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("message info is not matched", AttemptRolloverStep.getCopyAliasRolledOverIndexNotFoundMessage(oldIndexName), updatedManagedIndexMetaData.info?.get("message"))
            verify(rolloverActionMetrics.successes).add(eq(1.0), any<Tags>())
        }
    }

    fun `test copy alias in rollover step with no additional aliases to copy`() {
        val rolloverResponse = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, true, true)
        val client = getClient(getAdminClient(getIndicesAdminClient(rolloverResponse, null, null, null)))

        // Override setup to have only rollover alias (no additional aliases to copy)
        val metadata = clusterService.state().metadata()
        val indexMetadata = metadata.index(oldIndexName)
        whenever(indexMetadata.aliases).thenReturn(mapOf(alias to AliasMetadata.builder(alias).build()))

        runBlocking {
            val rolloverAction = RolloverAction(null, null, null, null, true, false, index = 0)
            val managedIndexMetaData =
                ManagedIndexMetaData(
                    oldIndexName, "indexUuid", "policy_id",
                    null, null, null,
                    null, null, null,
                    null, ActionMetaData("rollover", Instant.now().toEpochMilli(), 0, false, 1, null, null), null,
                    null, null, rolledOverIndexName = newIndexName,
                )
            val attemptRolloverStep = AttemptRolloverStep(rolloverAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptRolloverStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptRolloverStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptRolloverStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("message info is not matched", AttemptRolloverStep.getSuccessNoAliasesToCopyMessage(oldIndexName, newIndexName), updatedManagedIndexMetaData.info?.get("message"))
            verify(rolloverActionMetrics.successes).add(eq(1.0), any<Tags>())
        }
    }

    fun `test failed rollover with copy alias preserves failure`() {
        val exception = IllegalArgumentException("rollover failed")
        for (isDataStream in listOf(false, true)) {
            for (rolledOverIndexName in listOf(null, newIndexName)) {
                val (metadata, indicesClient) = executeRolloverWithCopyAlias(
                    rolloverException = exception,
                    isDataStream = isDataStream,
                    rolledOverIndexName = rolledOverIndexName,
                )
                assertEquals(Step.StepStatus.FAILED, metadata.stepMetaData?.stepStatus)
                assertEquals(AttemptRolloverStep.getFailedMessage(oldIndexName), metadata.info?.get("message"))
                assertEquals(exception.message, metadata.info?.get("cause"))
                assertEquals(false, metadata.rolledOver)
                assertEquals(rolledOverIndexName, metadata.rolledOverIndexName)
                verify(indicesClient, never()).aliases(any(), any())
            }
        }
        verify(rolloverActionMetrics.failures, times(4)).add(eq(1.0), any<Tags>())
        verify(rolloverActionMetrics.successes, never()).add(any(), any<Tags>())
    }

    fun `test unacknowledged rollover with copy alias preserves failure`() {
        for (isDataStream in listOf(false, true)) {
            val response = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, false, false)
            val (metadata, indicesClient) = executeRolloverWithCopyAlias(
                rolloverResponse = response,
                isDataStream = isDataStream,
                rolledOverIndexName = newIndexName,
            )
            val message = if (isDataStream) {
                AttemptRolloverStep.getFailedDataStreamRolloverMessage("logs-stream")
            } else {
                AttemptRolloverStep.getFailedAliasUpdateMessage(oldIndexName, newIndexName)
            }
            assertEquals(Step.StepStatus.FAILED, metadata.stepMetaData?.stepStatus)
            assertEquals(message, metadata.info?.get("message"))
            assertEquals(false, metadata.rolledOver)
            verify(indicesClient, never()).aliases(any(), any())
        }
        verify(rolloverActionMetrics.failures, times(2)).add(eq(1.0), any<Tags>())
        verify(rolloverActionMetrics.successes, never()).add(any(), any<Tags>())
    }

    fun `test successful rollover copies additional aliases`() {
        val response = RolloverResponse(oldIndexName, newIndexName, mapOf(), false, true, true, true)
        val (metadata, indicesClient) = executeRolloverWithCopyAlias(rolloverResponse = response)
        assertEquals(Step.StepStatus.COMPLETED, metadata.stepMetaData?.stepStatus)
        assertEquals(AttemptRolloverStep.getSuccessCopyAliasMessage(oldIndexName, newIndexName), metadata.info?.get("message"))
        assertEquals(true, metadata.rolledOver)
        assertEquals(newIndexName, metadata.rolledOverIndexName)
        val request = argumentCaptor<IndicesAliasesRequest>()
        verify(indicesClient).aliases(request.capture(), any())
        val aliasAction = request.firstValue.aliasActions.single()
        assertArrayEquals(arrayOf(additionalAlias), aliasAction.aliases())
        assertArrayEquals(arrayOf(newIndexName), aliasAction.indices())
        verify(rolloverActionMetrics.successes).add(eq(1.0), any<Tags>())
    }

    private fun executeRolloverWithCopyAlias(
        rolloverResponse: RolloverResponse? = null,
        rolloverException: Exception? = null,
        isDataStream: Boolean = false,
        rolledOverIndexName: String? = null,
    ): Pair<ManagedIndexMetaData, IndicesAdminClient> {
        val indicesClient = getIndicesAdminClient(rolloverResponse, AcknowledgedResponse(true), rolloverException, null)
        stubRolloverPrerequisites(indicesClient, isDataStream)
        val action = RolloverAction(null, null, null, null, copyAlias = true, index = 0)
        val managedIndexMetaData = ManagedIndexMetaData(
            oldIndexName, "indexUuid", "policy_id",
            null, null, null,
            null, null, null,
            null, ActionMetaData("rollover", Instant.now().toEpochMilli(), 0, false, 1, null, null), null,
            null, null, rolledOverIndexName = rolledOverIndexName,
        )
        val step = AttemptRolloverStep(action)
        val client = getClient(getAdminClient(indicesClient))
        val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
        runBlocking {
            step.preExecute(logger, context).execute()
                .postExecute(logger, IndexManagementActionsMetrics.instance, step, managedIndexMetaData)
        }
        return step.getUpdatedManagedIndexMetadata(managedIndexMetaData) to indicesClient
    }

    private fun stubRolloverPrerequisites(indicesClient: IndicesAdminClient, isDataStream: Boolean) {
        val metadata = clusterService.state().metadata()
        whenever(metadata.index(oldIndexName).rolloverInfos).thenReturn(emptyMap())
        if (isDataStream) {
            val dataStream: IndexAbstraction.DataStream = mock()
            val indexAbstraction: IndexAbstraction = mock()
            whenever(dataStream.name).thenReturn("logs-stream")
            whenever(indexAbstraction.parentDataStream).thenReturn(dataStream)
            whenever(metadata.indicesLookup).thenReturn(sortedMapOf(oldIndexName to indexAbstraction))
        } else {
            whenever(metadata.indicesLookup).thenReturn(sortedMapOf())
        }
        val statsResponse: IndicesStatsResponse = mock()
        whenever(statsResponse.status).thenReturn(RestStatus.OK)
        whenever(statsResponse.primaries).thenReturn(CommonStats())
        whenever(statsResponse.shards).thenReturn(emptyArray())
        doAnswer { invocation ->
            invocation.getArgument<ActionListener<IndicesStatsResponse>>(1).onResponse(statsResponse)
        }.whenever(indicesClient).stats(any(), any())
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
        // Allow both aliasResponse and aliasException to be null (for tests where aliases() is not called)
        if (aliasResponse != null || aliasException != null) {
            assertTrue(
                "Must provide one and only one response or exception",
                (aliasResponse != null).xor(aliasException != null),
            )
        }
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<RolloverResponse>>(1)
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
                } else if (aliasException != null) {
                    listener.onFailure(aliasException)
                }
                // If both are null, do nothing (for tests where aliases() should not be called)
            }.whenever(this.mock).aliases(any(), any())
        }
    }
}
