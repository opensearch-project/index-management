/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.spy
import kotlinx.coroutines.runBlocking
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.step.unfollow.AttemptUnfollowStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase

/*import com.nhaarman.mockitokotlin2.whenever
import org.opensearch.commons.replication.ReplicationPluginInterface
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.opensearchapi.suspendUntil*/
class AttemptUnfollowStepTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)

    fun `test unfollownew step sets step status to completed when successful`() {
        val mockNodeClient = Mockito.mock(NodeClient::class.java)
        // val request = StopIndexReplicationRequest("test")
        val successfulResponse = AcknowledgedResponse(true)
        /*Mockito.mockStatic(ReplicationPluginInterface::class.java).use {
            whenever(
                runBlocking {
                    ReplicationPluginInterface.suspendUntil<AcknowledgedResponse>(
                        Mockito.any(),
                    )
                },
            ).thenAnswer { invocation ->
                val actionListener = invocation.getArgument<ActionListener<AcknowledgedResponse>>(0)
                actionListener.onResponse(successfulResponse)
                successfulResponse
            }*/
        runBlocking {
            // val attemptUnfollowStep = AttemptUnfollowStep()
            val attemptUnfollowStep = spy(AttemptUnfollowStep())
            `when`(attemptUnfollowStep.performStopAction(any(), any())).thenReturn(successfulResponse)
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
                mockNodeClient,
                null,
                null,
                scriptService,
                settings,
                lockService,
            )
            attemptUnfollowStep.preExecute(logger, context).execute()
            val updatedManagedIndexMetaData = attemptUnfollowStep.getUpdatedManagedIndexMetadata(managedIndexMetaData)
            assertEquals(
                "Step status is not COMPLETED",
                Step.StepStatus.COMPLETED,
                updatedManagedIndexMetaData.stepMetaData?.stepStatus,
            )
        }
    }
}
