/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexAbstraction
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.validation.ValidateRollover.Companion.getFailedNoValidAliasMessage
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.monitor.jvm.JvmService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import java.util.*

class ValidateRolloverTests : OpenSearchTestCase() {
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val clusterService: ClusterService = mock()
    private val jvmService: JvmService = mock()
    private val indexName: String = "test"
    private val metadata = ManagedIndexMetaData(
        indexName, "indexUuid", "policy_id", null, null, null, null, null, null, null,
        ActionMetaData
        ("rollover", 1, 0, false, 0, null, null),
        null, null, null
    )
    val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)
    private val client: Client = mock()
    private val lockService: LockService = LockService(mock(), clusterService)
    private val validate = ValidateRollover(settings, clusterService, jvmService)
    private val clusterState: ClusterState = mock()
    private val clusterServiceMetadata: Metadata = mock()
    private val indexAbstraction: IndexAbstraction = mock()
    private val indicesLookup: SortedMap<String, IndexAbstraction> = mock()
    private val listOfMetadata: MutableList<IndexMetadata> = mock()
    private val indexMetadata: IndexMetadata = mock()

    fun `test rollover when missing rollover alias`() {
        val metadata = metadata.copy()
        val context = StepContext(metadata, clusterService, client, null, null, scriptService, settings, lockService)
        whenever(context.clusterService.state()).thenReturn(clusterState)
        whenever(clusterState.metadata()).thenReturn(clusterServiceMetadata)
        whenever(clusterServiceMetadata.indicesLookup).thenReturn(indicesLookup)
        whenever(indicesLookup[indexName]).thenReturn(indexAbstraction)
        whenever(indexAbstraction.indices).thenReturn(listOfMetadata)
        whenever(clusterServiceMetadata.index(indexName)).thenReturn(indexMetadata)
        whenever(indexMetadata.settings).thenReturn(settings)

        // null pointer exception
        runBlocking {
            validate.execute(indexName)
        }
        assertEquals("Validation status is RE_VALIDATING", Validate.ValidationStatus.RE_VALIDATING, validate.validationStatus)
        assertEquals("Info message is NO VALID ALIAS", getFailedNoValidAliasMessage(indexName), validate.validationMessage)
    }
}
