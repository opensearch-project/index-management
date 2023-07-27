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
import org.junit.Assert
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.ClusterAdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.randomNotificationActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.notification.AttemptNotificationStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase

class AttemptNotificationStepTests : OpenSearchTestCase() {
    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val notificationAction = randomNotificationActionConfig()
    private val metadata = ManagedIndexMetaData(
        "test", "indexUuid", "policy_id", null, null, null, null, null, null, null,
        ActionMetaData(AttemptNotificationStep.name, 1, 0, false, 0, null, ActionProperties()), null, null, null
    )
    private val lockService: LockService = LockService(mock(), clusterService)

    suspend fun `test detect transient failure`() {
        // TODO adjust this test after implementing isTransientFailure method
        val step = AttemptNotificationStep(notificationAction)
        val client = getClient(getAdminClient(getClusterAdminClient(null, null)))
        val stepContext = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
        Assert.assertEquals("Cannot detect transient failure", false, step.isTransientFailure(client, stepContext, metadata))
    }
    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }
    private fun getAdminClient(clusterAdminClient: ClusterAdminClient): AdminClient = mock { on { cluster() } doReturn clusterAdminClient }
    private fun getClusterAdminClient(createSnapshotRequest: CreateSnapshotResponse?, exception: Exception?): ClusterAdminClient {
        OpenSearchTestCase.assertTrue(
            "Must provide one and only one response or exception",
            (createSnapshotRequest != null).xor(exception != null)
        )
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<CreateSnapshotResponse>>(1)
                if (createSnapshotRequest != null) listener.onResponse(createSnapshotRequest)
                else listener.onFailure(exception)
            }.whenever(this.mock).createSnapshot(any(), any())
        }
    }
}
