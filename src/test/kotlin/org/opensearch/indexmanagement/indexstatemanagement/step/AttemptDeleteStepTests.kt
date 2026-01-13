/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.doAnswer
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.step.delete.AttemptDeleteStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.IndexManagementActionsMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.metrics.actionmetrics.DeleteActionMetrics
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.telemetry.metrics.Counter
import org.opensearch.telemetry.metrics.MetricsRegistry
import org.opensearch.telemetry.metrics.tags.Tags
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.ClusterAdminClient
import org.opensearch.transport.client.IndicesAdminClient
import java.time.Instant

class AttemptDeleteStepTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = mock()
    private lateinit var metricsRegistry: MetricsRegistry
    private lateinit var deleteActionMetrics: DeleteActionMetrics

    @Before
    fun setup() {
        metricsRegistry = mock()
        whenever(metricsRegistry.createCounter(anyString(), anyString(), anyString())).thenAnswer {
            mock<Counter>()
        }
        IndexManagementActionsMetrics.instance.initialize(metricsRegistry)
        ManagedIndexRunner.registerIndexManagementActionMetrics(IndexManagementActionsMetrics.instance)
        deleteActionMetrics = IndexManagementActionsMetrics.instance.getActionMetrics(IndexManagementActionsMetrics.DELETE) as DeleteActionMetrics
    }

    fun `test delete step sets step status to completed when successful`() {
        val acknowledgedResponse = AcknowledgedResponse(true)
        val client = getClient(getAdminClient(getIndicesAdminClient(acknowledgedResponse, null)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData(
                "test", "indexUuid", "policy_id", null, null, null, null, null, null, null,
                ActionMetaData("delete", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null,
            )
            val deleteAction = DeleteAction(0)
            val attemptDeleteStep = AttemptDeleteStep(deleteAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptDeleteStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            verify(deleteActionMetrics.successes).add(eq(1.0), any<Tags>())
        }
    }

    fun `test delete step sets step status to failed when not acknowledged`() {
        val acknowledgedResponse = AcknowledgedResponse(false)
        val client = getClient(getAdminClient(getIndicesAdminClient(acknowledgedResponse, null)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData("delete", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null)
            val deleteAction = DeleteAction(0)
            val attemptDeleteStep = AttemptDeleteStep(deleteAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptDeleteStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            verify(deleteActionMetrics.failures).add(eq(1.0), any<Tags>())
        }
    }

    fun `test delete step sets step status to failed when error thrown`() {
        val exception = IllegalArgumentException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData("delete", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null)
            val deleteAction = DeleteAction(0)
            val attemptDeleteStep = AttemptDeleteStep(deleteAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptDeleteStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            logger.info(updatedManagedIndexMetaData)
            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            verify(deleteActionMetrics.failures).add(eq(1.0), any<Tags>())
        }
    }

    fun `test delete step sets step status to condition not met when snapshot in progress error thrown`() {
        val exception = SnapshotInProgressException("example")
        val client = getClient(getAdminClient(getIndicesAdminClient(null, exception)))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData("test", "indexUuid", "policy_id", null, null, null, null, null, null, null, ActionMetaData("delete", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null)
            val deleteAction = DeleteAction(0)
            val attemptDeleteStep = AttemptDeleteStep(deleteAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptDeleteStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals("Step status is not CONDITION_NOT_MET", Step.StepStatus.CONDITION_NOT_MET, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }

    private fun getAdminClient(indicesAdminClient: IndicesAdminClient, clusterAdminClient: ClusterAdminClient): AdminClient = mock {
        on { indices() } doReturn indicesAdminClient
        on { cluster() } doReturn clusterAdminClient
    }

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
            }.whenever(this.mock).delete(any(), any())
        }
    }

    private fun getIndicesAdminClient(
        deleteResponse: AcknowledgedResponse?,
        deleteException: Exception?,
        getSettingsResponse: GetSettingsResponse?,
    ): IndicesAdminClient = mock {
        doAnswer { invocationOnMock ->
            val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
            if (deleteResponse != null) {
                listener.onResponse(deleteResponse)
            } else {
                listener.onFailure(deleteException)
            }
        }.whenever(this.mock).delete(any(), any())

        doAnswer { invocationOnMock ->
            val listener = invocationOnMock.getArgument<ActionListener<GetSettingsResponse>>(1)
            listener.onResponse(getSettingsResponse)
        }.whenever(this.mock).getSettings(any(), any())
    }

    private fun getClusterAdminClient(deleteSnapshotResponse: AcknowledgedResponse): ClusterAdminClient = mock {
        doAnswer { invocationOnMock ->
            val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
            listener.onResponse(deleteSnapshotResponse)
        }.whenever(this.mock).deleteSnapshot(any(), any())
    }

    private fun createGetSettingsResponse(indexName: String, settings: Settings): GetSettingsResponse {
        val indexToSettings = mapOf(indexName to settings)
        return mock {
            on { this.indexToSettings } doReturn indexToSettings
        }
    }

    fun `test delete step with delete_snapshot deletes both index and snapshot`() {
        val indexName = "test-searchable-snapshot"
        val repository = "my-repo"
        val snapshotName = "my-snapshot"

        val indexSettings = Settings.builder()
            .put("index.searchable_snapshot.repository", repository)
            .put("index.searchable_snapshot.snapshot_id.name", snapshotName)
            .build()
        val getSettingsResponse = createGetSettingsResponse(indexName, indexSettings)

        val indicesAdminClient = getIndicesAdminClient(AcknowledgedResponse(true), null, getSettingsResponse)
        val clusterAdminClient = getClusterAdminClient(AcknowledgedResponse(true))
        val client = getClient(getAdminClient(indicesAdminClient, clusterAdminClient))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData(
                indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
                ActionMetaData("delete", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null,
            )
            val deleteAction = DeleteAction(0, deleteSnapshot = true)
            val attemptDeleteStep = AttemptDeleteStep(deleteAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptDeleteStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)

            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            assertTrue(
                "Info message should mention snapshot deletion",
                updatedManagedIndexMetaData.info?.get("message").toString().contains("snapshot"),
            )
            verify(clusterAdminClient).deleteSnapshot(any(), any())
        }
    }

    fun `test delete step with delete_snapshot skips snapshot deletion for non-searchable-snapshot index`() {
        val indexName = "test-normal-index"

        // Normal index without searchable snapshot settings
        val indexSettings = Settings.builder().build()
        val getSettingsResponse = createGetSettingsResponse(indexName, indexSettings)

        val indicesAdminClient = getIndicesAdminClient(AcknowledgedResponse(true), null, getSettingsResponse)
        val clusterAdminClient: ClusterAdminClient = mock()
        val client = getClient(getAdminClient(indicesAdminClient, clusterAdminClient))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData(
                indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
                ActionMetaData("delete", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null,
            )
            val deleteAction = DeleteAction(0, deleteSnapshot = true)
            val attemptDeleteStep = AttemptDeleteStep(deleteAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptDeleteStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)

            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            verify(clusterAdminClient, never()).deleteSnapshot(any(), any())
        }
    }

    fun `test delete step with delete_snapshot skips snapshot deletion when snapshot is used by other index`() {
        val indexName = "test-searchable-snapshot"
        val otherIndexName = "other-searchable-snapshot"
        val repository = "my-repo"
        val snapshotName = "my-snapshot"

        // Both indices use the same snapshot
        val indexSettings = Settings.builder()
            .put("index.searchable_snapshot.repository", repository)
            .put("index.searchable_snapshot.snapshot_id.name", snapshotName)
            .build()

        // Mock getSettings to return both indices when querying "*"
        val allIndicesSettings = mapOf(
            indexName to indexSettings,
            otherIndexName to indexSettings,
        )
        val getSettingsResponseForIndex: GetSettingsResponse = mock {
            on { this.indexToSettings } doReturn mapOf(indexName to indexSettings)
        }
        val getSettingsResponseForAll: GetSettingsResponse = mock {
            on { this.indexToSettings } doReturn allIndicesSettings
        }

        val indicesAdminClient: IndicesAdminClient = mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
                listener.onResponse(AcknowledgedResponse(true))
            }.whenever(this.mock).delete(any(), any())

            doAnswer { invocationOnMock ->
                val request = invocationOnMock.getArgument<GetSettingsRequest>(0)
                val listener = invocationOnMock.getArgument<ActionListener<GetSettingsResponse>>(1)
                // Return different response based on indices pattern
                if (request.indices().contentEquals(arrayOf("*"))) {
                    listener.onResponse(getSettingsResponseForAll)
                } else {
                    listener.onResponse(getSettingsResponseForIndex)
                }
            }.whenever(this.mock).getSettings(any(), any())
        }

        val clusterAdminClient: ClusterAdminClient = mock()
        val client = getClient(getAdminClient(indicesAdminClient, clusterAdminClient))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData(
                indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
                ActionMetaData("delete", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null,
            )
            val deleteAction = DeleteAction(0, deleteSnapshot = true)
            val attemptDeleteStep = AttemptDeleteStep(deleteAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptDeleteStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)

            // Index should be deleted successfully, but snapshot deletion should be skipped
            assertEquals("Step status is not COMPLETED", Step.StepStatus.COMPLETED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            verify(clusterAdminClient, never()).deleteSnapshot(any(), any())
        }
    }

    fun `test delete step with delete_snapshot fails when snapshot deletion is not acknowledged`() {
        val indexName = "test-searchable-snapshot"
        val repository = "my-repo"
        val snapshotName = "my-snapshot"

        val indexSettings = Settings.builder()
            .put("index.searchable_snapshot.repository", repository)
            .put("index.searchable_snapshot.snapshot_id.name", snapshotName)
            .build()
        val getSettingsResponse = createGetSettingsResponse(indexName, indexSettings)

        val indicesAdminClient = getIndicesAdminClient(AcknowledgedResponse(true), null, getSettingsResponse)
        val clusterAdminClient = getClusterAdminClient(AcknowledgedResponse(false))
        val client = getClient(getAdminClient(indicesAdminClient, clusterAdminClient))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData(
                indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
                ActionMetaData("delete", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null,
            )
            val deleteAction = DeleteAction(0, deleteSnapshot = true)
            val attemptDeleteStep = AttemptDeleteStep(deleteAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptDeleteStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)

            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            verify(deleteActionMetrics.failures).add(eq(1.0), any<Tags>())
        }
    }

    fun `test delete step with delete_snapshot fails when snapshot deletion throws exception`() {
        val indexName = "test-searchable-snapshot"
        val repository = "my-repo"
        val snapshotName = "my-snapshot"

        val indexSettings = Settings.builder()
            .put("index.searchable_snapshot.repository", repository)
            .put("index.searchable_snapshot.snapshot_id.name", snapshotName)
            .build()
        val getSettingsResponse = createGetSettingsResponse(indexName, indexSettings)

        val indicesAdminClient = getIndicesAdminClient(AcknowledgedResponse(true), null, getSettingsResponse)
        val clusterAdminClient: ClusterAdminClient = mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
                listener.onFailure(RuntimeException("Snapshot deletion failed"))
            }.whenever(this.mock).deleteSnapshot(any(), any())
        }
        val client = getClient(getAdminClient(indicesAdminClient, clusterAdminClient))

        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData(
                indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
                ActionMetaData("delete", Instant.now().toEpochMilli(), 0, false, 1, null, null), null, null, null,
            )
            val deleteAction = DeleteAction(0, deleteSnapshot = true)
            val attemptDeleteStep = AttemptDeleteStep(deleteAction)
            val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)
            attemptDeleteStep.preExecute(logger, context).execute().postExecute(logger, IndexManagementActionsMetrics.instance, attemptDeleteStep, managedIndexMetaData)
            val updatedManagedIndexMetaData = attemptDeleteStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)

            assertEquals("Step status is not FAILED", Step.StepStatus.FAILED, updatedManagedIndexMetaData.stepMetaData?.stepStatus)
            verify(deleteActionMetrics.failures).add(eq(1.0), any<Tags>())
        }
    }
}
