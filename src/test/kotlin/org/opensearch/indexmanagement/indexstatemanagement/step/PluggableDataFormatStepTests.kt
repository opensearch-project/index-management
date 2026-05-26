/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.indexmanagement.indexstatemanagement.action.TransformAction
import org.opensearch.indexmanagement.indexstatemanagement.step.transform.AttemptCreateTransformJobStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.transform.randomISMTransform
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.Client

class PluggableDataFormatStepTests : OpenSearchTestCase() {

    fun `test transform step fails on pluggable dataformat index`() {
        val indexName = "pluggable-index"
        val ismTransform = randomISMTransform()
        val action = TransformAction(ismTransform = ismTransform, index = 0)
        val step = AttemptCreateTransformJobStep(action)

        val indexMetadata: IndexMetadata = mock()
        whenever(indexMetadata.settings).doReturn(
            Settings.builder().put("index.pluggable.dataformat.enabled", true).build(),
        )
        val metadata: Metadata = mock()
        whenever(metadata.index(indexName)).doReturn(indexMetadata)
        val clusterState: ClusterState = mock()
        whenever(clusterState.metadata()).doReturn(metadata)
        val clusterService: ClusterService = mock()
        whenever(clusterService.state()).doReturn(clusterState)

        val managedIndexMetaData = ManagedIndexMetaData(
            indexName, "uuid", "policy", null, null, null, null, null, null, null, null, null, null, null,
        )
        val context = StepContext(
            managedIndexMetaData, clusterService, mock<Client>(), ThreadContext(Settings.EMPTY),
            null, mock<ScriptService>(), Settings.EMPTY, mock<LockService>(),
        )

        step.preExecute(logger, context)
        runBlocking { step.execute() }

        val updated = step.getUpdatedManagedIndexMetadata(managedIndexMetaData)
        assertEquals(Step.StepStatus.FAILED, updated.stepMetaData?.stepStatus)
        assertTrue(
            "Error should mention Optimized Engine",
            updated.info?.get("cause").toString().contains("Optimized Engine"),
        )
    }
}
