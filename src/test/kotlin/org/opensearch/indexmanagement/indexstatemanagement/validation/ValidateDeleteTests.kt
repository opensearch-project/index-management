/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.opensearch.test.OpenSearchTestCase
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.ImmutableOpenMap
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate

class ValidateDeleteTests : OpenSearchTestCase() {
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val clusterService: ClusterService = mock()
    private val indexName: String = "test"
    private val metadata = ManagedIndexMetaData(
        indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
        ActionMetaData
        ("delete", 1, 0, false, 0, null, null),
        null, null, null, null
    )
    // val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)
    private val client: Client = mock()
    private val lockService: LockService = LockService(mock(), clusterService)
    private val validate = ValidateDelete(settings, clusterService)
    private val clusterState: ClusterState = mock()
    private val clusterServiceMetadata: Metadata = mock() // don't mock this?
    private val indices: ImmutableOpenMap<String, IndexMetadata> = mock()

    fun `test delete index exists`() {
        val actionMetadata = metadata.actionMetaData!!.copy()
        val metadata = metadata.copy()
        val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
        whenever(context.clusterService.state()).thenReturn(clusterState)
        whenever(clusterState.metadata).thenReturn(clusterServiceMetadata)
        whenever(clusterServiceMetadata.indices).thenReturn(indices)
        whenever(indices.containsKey(indexName)).thenReturn(false)

        // null pointer exception
        runBlocking {
            validate.executeValidation(indexName)
        }

        validate.getUpdatedManagedIndexMetadata(metadata, actionMetadata)
        assertEquals("Validation status is REVALIDATE", Validate.ValidationStatus.REVALIDATE, validate.validationStatus)
    }
}
