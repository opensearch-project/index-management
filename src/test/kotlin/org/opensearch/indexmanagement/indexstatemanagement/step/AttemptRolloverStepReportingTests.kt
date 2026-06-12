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
import org.opensearch.action.admin.indices.stats.CommonStats
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.shard.DocsStats
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverConditionGroup
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import java.time.Instant

/**
 * Tests for AttemptRolloverStep's reporting of the evaluated rollover criteria (flat vs grouped),
 * and for the prevent_empty_rollover guard interaction with grouped conditions.
 */
class AttemptRolloverStepReportingTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = mock()
    private val indexName = "test_index"
    private val alias = "alias"

    @Before
    fun setup() {
        val metricsRegistry: MetricsRegistry = mock()
        whenever(metricsRegistry.createCounter(anyString(), anyString(), anyString())).thenAnswer { mock<Counter>() }
        IndexManagementActionsMetrics.instance.initialize(metricsRegistry)
        ManagedIndexRunner.registerIndexManagementActionMetrics(IndexManagementActionsMetrics.instance)

        val clusterState: ClusterState = mock()
        val metadata: Metadata = mock()
        val indexMetadata: IndexMetadata = mock()
        val indexSettings =
            Settings.builder()
                .put(ManagedIndexSettings.ROLLOVER_ALIAS.key, alias)
                .put(ManagedIndexSettings.ROLLOVER_SKIP.key, false)
                .build()
        whenever(clusterService.state()).thenReturn(clusterState)
        whenever(clusterState.metadata()).thenReturn(metadata)
        whenever(clusterState.metadata).thenReturn(metadata)
        whenever(metadata.index(indexName)).thenReturn(indexMetadata)
        whenever(metadata.indicesLookup).thenReturn(sortedMapOf())
        whenever(indexMetadata.settings).thenReturn(indexSettings)
        // Not already rolled over, so condition evaluation/reporting is reached.
        whenever(indexMetadata.rolloverInfos).thenReturn(mapOf())
        whenever(indexMetadata.creationDate).thenReturn(Instant.now().toEpochMilli())
        // Alias exists and this index is its write index, so preCheckIndexAlias passes.
        whenever(indexMetadata.aliases).thenReturn(
            mapOf(alias to AliasMetadata.builder(alias).writeIndex(true).build()),
        )
    }

    private fun statsResponse(numDocs: Long, sizeInBytes: Long): IndicesStatsResponse {
        val commonStats = CommonStats()
        commonStats.docs = DocsStats.Builder().count(numDocs).deleted(0).totalSizeInBytes(sizeInBytes).build()
        return mock {
            on { status } doReturn RestStatus.OK
            on { primaries } doReturn commonStats
            on { shards } doReturn arrayOf()
        }
    }

    private fun clientReturningStats(statsResponse: IndicesStatsResponse): Client {
        val indicesAdminClient: IndicesAdminClient = mock {
            doAnswer { invocation ->
                val listener = invocation.getArgument<ActionListener<IndicesStatsResponse>>(1)
                listener.onResponse(statsResponse)
            }.whenever(this.mock).stats(any(), any())
        }
        val adminClient: AdminClient = mock { on { indices() } doReturn indicesAdminClient }
        return mock { on { admin() } doReturn adminClient }
    }

    private fun metadata(): ManagedIndexMetaData = ManagedIndexMetaData(
        indexName, "indexUuid", "policy_id",
        null, null, null,
        null, null, null,
        null, ActionMetaData("rollover", Instant.now().toEpochMilli(), 0, false, 1, null, null), null,
        null, null,
    )

    @Suppress("UNCHECKED_CAST")
    private fun runStep(action: RolloverAction, statsResponse: IndicesStatsResponse): ManagedIndexMetaData {
        val client = clientReturningStats(statsResponse)
        val managedIndexMetaData = metadata()
        val step = AttemptRolloverStep(action)
        val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
        runBlocking {
            step.preExecute(logger, context).execute()
        }
        return step.getUpdatedManagedIndexMetadata(managedIndexMetaData)
    }

    @Suppress("UNCHECKED_CAST")
    fun `test grouped action reports any_of in conditions`() {
        // Use thresholds that are not met so the step reports CONDITION_NOT_MET with the conditions map.
        val action = RolloverAction(
            minSize = null, minDocs = null, minAge = null, minPrimaryShardSize = null,
            conditionGroups = listOf(
                RolloverConditionGroup(
                    minSize = null, minDocs = 1_000_000L,
                    minAge = TimeValue.timeValueDays(30), minPrimaryShardSize = null,
                ),
            ),
            index = 0,
        )
        val updated = runStep(action, statsResponse(numDocs = 0, sizeInBytes = 0))

        assertEquals("Should be CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updated.stepMetaData?.stepStatus)
        val conditions = updated.info?.get("conditions") as? Map<String, Any?>
        assertNotNull("conditions should be reported", conditions)
        assertTrue("conditions should contain any_of", conditions!!.containsKey(RolloverAction.ANY_OF_FIELD))
    }

    @Suppress("UNCHECKED_CAST")
    fun `test flat action reports flat conditions`() {
        val action = RolloverAction(
            minSize = null, minDocs = 1_000_000L, minAge = null, minPrimaryShardSize = null,
            index = 0,
        )
        val updated = runStep(action, statsResponse(numDocs = 0, sizeInBytes = 0))

        assertEquals("Should be CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updated.stepMetaData?.stepStatus)
        val conditions = updated.info?.get("conditions") as? Map<String, Any?>
        assertNotNull("conditions should be reported", conditions)
        assertTrue("conditions should contain min_doc_count", conditions!!.containsKey(RolloverAction.MIN_DOC_COUNT_FIELD))
        assertFalse("flat conditions should NOT contain any_of", conditions.containsKey(RolloverAction.ANY_OF_FIELD))
    }

    fun `test prevent empty rollover guard applies to grouped action`() {
        val groupedAction = RolloverAction(
            minSize = null, minDocs = null, minAge = null, minPrimaryShardSize = null,
            preventEmptyRollover = true,
            conditionGroups = listOf(RolloverConditionGroup(null, 1L, null, null)),
            index = 0,
        )
        val updatedGrouped = runStep(groupedAction, statsResponse(numDocs = 0, sizeInBytes = 0))
        assertEquals(
            "Empty index with prevent_empty_rollover must be CONDITION_NOT_MET (grouped)",
            Step.StepStatus.CONDITION_NOT_MET, updatedGrouped.stepMetaData?.stepStatus,
        )
        assertEquals(
            "Empty-index guard message should be reported",
            AttemptRolloverStep.getPreventedEmptyRolloverMessage(indexName), updatedGrouped.info?.get("message"),
        )

        val flatAction = RolloverAction(
            minSize = null, minDocs = 1L, minAge = null, minPrimaryShardSize = null,
            preventEmptyRollover = true,
            index = 0,
        )
        val updatedFlat = runStep(flatAction, statsResponse(numDocs = 0, sizeInBytes = 0))
        assertEquals(
            "Empty index with prevent_empty_rollover must be CONDITION_NOT_MET (flat)",
            Step.StepStatus.CONDITION_NOT_MET, updatedFlat.stepMetaData?.stepStatus,
        )
    }
}
