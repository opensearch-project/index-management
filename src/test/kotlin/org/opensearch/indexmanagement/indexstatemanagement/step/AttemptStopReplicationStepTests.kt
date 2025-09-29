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
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.indexstatemanagement.step.stopreplication.AttemptStopReplicationStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.node.NodeClient

class AttemptStopReplicationStepTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = mock()

    fun `test stop replication step sets step status to completed when successful`() {
        val client = getClient(true, false) // Simulate a successful response
        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData(
                "test",
                "indexUuid",
                "policy_id",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
            )
            val context = StepContext(
                managedIndexMetaData,
                clusterService,
                client,
                null,
                null,
                scriptService,
                settings,
                lockService,
            )
            val attemptStopReplicationStep = AttemptStopReplicationStep()
            attemptStopReplicationStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptStopReplicationStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals(
                "Step status is not COMPLETED",
                Step.StepStatus.COMPLETED,
                updatedManagedIndexMetaData.stepMetaData?.stepStatus,
            )
        }
    }

    fun `test stop replication step sets step status to failed when not acknowledged`() {
        val client = getClient(false, false) // Simulate a failed response
        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData(
                "test",
                "indexUuid",
                "policy_id",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
            )
            val context = StepContext(
                managedIndexMetaData,
                clusterService,
                client,
                null,
                null,
                scriptService,
                settings,
                lockService,
            )
            val attemptStopReplicationStep = AttemptStopReplicationStep()
            attemptStopReplicationStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptStopReplicationStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals(
                "Step status is not FAILED",
                Step.StepStatus.FAILED,
                updatedManagedIndexMetaData.stepMetaData?.stepStatus,
            )
        }
    }

    fun `test stop replication step sets step status to failed when error thrown`() {
        val client = getClient(true, true) // Simulate an exception
        runBlocking {
            val managedIndexMetaData = ManagedIndexMetaData(
                "test",
                "indexUuid",
                "policy_id",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
            )
            val context = StepContext(
                managedIndexMetaData,
                clusterService,
                client,
                null,
                null,
                scriptService,
                settings,
                lockService,
            )
            val attemptStopReplicationStep = AttemptStopReplicationStep()
            attemptStopReplicationStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptStopReplicationStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals(
                "Step status is not FAILED",
                Step.StepStatus.FAILED,
                updatedManagedIndexMetaData.stepMetaData?.stepStatus,
            )
        }
    }

    // Returns a mocked instance of NodeClient and customizes the behavior of execute()
    private fun getClient(ack: Boolean, exception: Boolean): PluginClient = mock<PluginClient> {
        // Mock the NodeClient that innerClient should return
        val inner: NodeClient = mock {
            doAnswer { inv ->
                val listener = inv.getArgument<ActionListener<AcknowledgedResponse>>(2)
                if (exception) {
                    listener.onFailure(Exception())
                } else {
                    listener.onResponse(AcknowledgedResponse(ack))
                }
                null
            }.whenever(this.mock).execute<StopIndexReplicationRequest, AcknowledgedResponse>(any(), any(), any())
        }

        // Mock PluginClient, return the inner NodeClient, and (optionally) also stub execute here
        return mock {
            on { innerClient() } doReturn inner
            doAnswer { inv ->
                val listener = inv.getArgument<ActionListener<AcknowledgedResponse>>(2)
                if (exception) {
                    listener.onFailure(Exception())
                } else {
                    listener.onResponse(AcknowledgedResponse(ack))
                }
                null
            }.whenever(this.mock).execute<StopIndexReplicationRequest, AcknowledgedResponse>(any(), any(), any())
        }
    }
}
