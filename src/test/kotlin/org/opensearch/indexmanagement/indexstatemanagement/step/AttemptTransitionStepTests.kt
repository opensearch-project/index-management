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
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.rollover.RolloverInfo
import org.opensearch.action.admin.indices.stats.CommonStats
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.ImmutableOpenMap
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.index.shard.DocsStats
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.action.TransitionsAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Conditions
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.transition.AttemptTransitionStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.rest.RestStatus
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

class AttemptTransitionStepTests : OpenSearchTestCase() {

    private val indexName: String = "test"
    private val indexUUID: String = "indexUuid"
    @Suppress("UNCHECKED_CAST")
    private val indexMetadata: IndexMetadata = mock {
        on { rolloverInfos } doReturn ImmutableOpenMap.builder<String, RolloverInfo>().build()
        on { indexUUID } doReturn indexUUID
    }
    private val metadata: Metadata = mock {
        on { index(any<String>()) } doReturn indexMetadata
        on { hasIndex(indexName) } doReturn true
    }
    private val clusterState: ClusterState = mock { on { metadata() } doReturn metadata }
    private val clusterService: ClusterService = mock { on { state() } doReturn clusterState }
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)

    private val docsStats: DocsStats = mock()
    private val primaries: CommonStats = mock { on { getDocs() } doReturn docsStats }
    private val statsResponse: IndicesStatsResponse = mock { on { primaries } doReturn primaries }

    @Before
    fun `setup settings`() {
        whenever(clusterService.clusterSettings).doReturn(ClusterSettings(Settings.EMPTY, setOf(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN)))
    }

    fun `test stats response not OK`() {
        whenever(indexMetadata.creationDate).doReturn(5L)
        whenever(statsResponse.status).doReturn(RestStatus.INTERNAL_SERVER_ERROR)
        whenever(statsResponse.shardFailures).doReturn(IndicesStatsResponse.EMPTY)
        whenever(docsStats.count).doReturn(6L)
        whenever(docsStats.totalSizeInBytes).doReturn(2)
        val client = getClient(getAdminClient(getIndicesAdminClient(statsResponse, null)))
        val indexMetadataProvider = IndexMetadataProvider(settings, client, clusterService, mutableMapOf())

        runBlocking {
            val managedIndexMetadata = ManagedIndexMetaData(indexName, indexUUID, "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val transitionsAction = TransitionsAction(listOf(Transition("some_state", Conditions(docCount = 5L))), indexMetadataProvider)
            val attemptTransitionStep = AttemptTransitionStep(transitionsAction)
            val context = StepContext(managedIndexMetadata, clusterService, client, null, null, scriptService, settings, lockService)
            attemptTransitionStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptTransitionStep.getUpdatedManagedIndexMetadata(managedIndexMetadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get correct failed message", AttemptTransitionStep.getFailedStatsMessage(indexName), updatedManagedIndexMetaData.info!!["message"])
        }
    }

    fun `test transitions fails on exception`() {
        whenever(indexMetadata.creationDate).doReturn(5L)
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))
        val indexMetadataProvider = IndexMetadataProvider(settings, client, clusterService, mutableMapOf())

        runBlocking {
            val managedIndexMetadata = ManagedIndexMetaData(indexName, indexUUID, "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val transitionsAction = TransitionsAction(listOf(Transition("some_state", Conditions(docCount = 5L))), indexMetadataProvider)
            val attemptTransitionStep = AttemptTransitionStep(transitionsAction)
            val context = StepContext(managedIndexMetadata, clusterService, client, null, null, scriptService, settings, lockService)
            attemptTransitionStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptTransitionStep.getUpdatedManagedIndexMetadata(managedIndexMetadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "example", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test transitions remote transport exception`() {
        whenever(indexMetadata.creationDate).doReturn(5L)
        val exception = RemoteTransportException("rte", IllegalArgumentException("nested"))
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))
        val indexMetadataProvider = IndexMetadataProvider(settings, client, clusterService, mutableMapOf())

        runBlocking {
            val managedIndexMetadata = ManagedIndexMetaData(indexName, indexUUID, "policy_id", null, null, null, null, null, null, null, null, null, null, null)
            val transitionsAction = TransitionsAction(listOf(Transition("some_state", Conditions(docCount = 5L))), indexMetadataProvider)
            val attemptTransitionStep = AttemptTransitionStep(transitionsAction)
            val context = StepContext(managedIndexMetadata, clusterService, client, null, null, scriptService, settings, lockService)
            attemptTransitionStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptTransitionStep.getUpdatedManagedIndexMetadata(managedIndexMetadata)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertEquals("Did not get cause from nested exception", "nested", updatedManagedIndexMetaData.info!!["cause"])
        }
    }

    fun `test step start time resetting between two transitions`() {
        val indexMetadataProvider = IndexMetadataProvider(settings, mock(), clusterService, mutableMapOf())
        runBlocking {
            val completedStartTime = Instant.now()
            val managedIndexMetadata = ManagedIndexMetaData(indexName, indexUUID, "policy_id", null, null, null, null, null, null, null, null, StepMetaData("attempt_transition", completedStartTime.toEpochMilli(), Step.StepStatus.COMPLETED), null, null)
            val transitionsAction = TransitionsAction(listOf(Transition("some_state", null)), indexMetadataProvider)
            val attemptTransitionStep = AttemptTransitionStep(transitionsAction)
            Thread.sleep(50) // Make sure we give enough time for the instants to be different
            val startTime = attemptTransitionStep.getStepStartTime(managedIndexMetadata)
            assertNotEquals("Two separate transitions should not have same start time", completedStartTime.toEpochMilli(), startTime.toEpochMilli())
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }
    private fun getIndicesAdminClient(statsResponse: IndicesStatsResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (statsResponse != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<IndicesStatsResponse>>(1)
                if (statsResponse != null) listener.onResponse(statsResponse)
                else listener.onFailure(exception)
            }.whenever(this.mock).stats(any(), any())
        }
    }
}
