/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.ClusterAdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.indexstatemanagement.randomRestoreActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.SNAPSHOT_DENY_LIST
import org.opensearch.indexmanagement.indexstatemanagement.step.restore.AttemptRestoreStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.ingest.TestTemplateService.MockTemplateScript
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.test.OpenSearchTestCase

class AttemptRestoreStepTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val restoreAction = randomRestoreActionConfig("repo")
    private val metadata = ManagedIndexMetaData(
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
        ActionMetaData(
            AttemptRestoreStep.name,
            1,
            0,
            false,
            0,
            null,
            ActionProperties(snapshotName = "snapshot-name"),
        ),
        null,
        null,
        null,
    )
    private val lockService: LockService = LockService(mock(), clusterService)

    @Before
    fun setup() {
        whenever(clusterService.clusterSettings).doReturn(ClusterSettings(Settings.EMPTY, setOf(SNAPSHOT_DENY_LIST)))
        whenever(scriptService.compile(any(), eq(TemplateScript.CONTEXT))).doReturn(MockTemplateScript.Factory("snapshot-pattern*"))
    }

    fun `test restore failure with no matching snapshots`() {
        val getSnapshotsResponse = GetSnapshotsResponse(emptyList())
        val client = getClient(getAdminClient(getClusterAdminClient(getSnapshotsResponse, null, null)))

        runBlocking {
            val step = AttemptRestoreStep(restoreAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedMetadata = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals(
                "Step status is not FAILED",
                Step.StepStatus.FAILED,
                updatedMetadata.stepMetaData?.stepStatus,
            )
            assertEquals(
                "Did not get correct failure message",
                "Failed to start restore for [index=test], cause: No snapshots found matching pattern [test*]",
                updatedMetadata.info!!["message"],
            )
        }
    }

    fun `test restore exception`() {
        val exception = IllegalArgumentException("example")
        val getSnapshotsResponse = GetSnapshotsResponse(emptyList())
        val client = getClient(getAdminClient(getClusterAdminClient(getSnapshotsResponse, null, exception)))

        runBlocking {
            val step = AttemptRestoreStep(restoreAction)
            val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
            step.preExecute(logger, context).execute()
            val updatedMetadata = step.getUpdatedManagedIndexMetadata(metadata)
            assertEquals(
                "Step status is not FAILED",
                Step.StepStatus.FAILED,
                updatedMetadata.stepMetaData?.stepStatus,
            )
            assertEquals(
                "Did not get cause from exception",
                "example",
                updatedMetadata.info!!["cause"],
            )
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(clusterAdminClient: ClusterAdminClient): AdminClient = mock { on { cluster() } doReturn clusterAdminClient }

    private fun getClusterAdminClient(
        getSnapshotsResponse: GetSnapshotsResponse?,
        restoreSnapshotResponse: RestoreSnapshotResponse?,
        exception: Exception?,
    ): ClusterAdminClient {
        return mock {
            // Mock getSnapshots call
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<GetSnapshotsResponse>>(1)
                when {
                    exception != null -> listener.onFailure(exception)
                    getSnapshotsResponse != null -> listener.onResponse(getSnapshotsResponse)
                    else -> listener.onResponse(GetSnapshotsResponse(emptyList()))
                }
            }.whenever(this.mock).getSnapshots(any(), any())

            // Mock restoreSnapshot call
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<RestoreSnapshotResponse>>(1)
                when {
                    exception != null -> listener.onFailure(exception)
                    restoreSnapshotResponse != null -> listener.onResponse(restoreSnapshotResponse)
                    else -> listener.onResponse(mock())
                }
            }.whenever(this.mock).restoreSnapshot(any(), any())
        }
    }
}
