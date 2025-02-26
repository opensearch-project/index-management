/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argThat
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.mockito.ArgumentMatcher
import org.mockito.Mockito.never
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForShrinkStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient

class WaitForShrinkStepTests : OpenSearchTestCase() {
    private val metadata: Metadata = mock {}
    private val clusterState: ClusterState = mock { on { metadata() } doReturn metadata }
    private val clusterService: ClusterService = mock { on { state() } doReturn clusterState }
    private val scriptService: ScriptService = mock()
    private val settings: Settings = Settings.EMPTY
    private val lockService: LockService = LockService(mock(), clusterService)
    private val ackedResponse = AcknowledgedResponse(true)
    private val unAckedResponse = AcknowledgedResponse(false)

    private val shrinkActionWithoutAliasesSwitch = ShrinkAction(numNewShards = 1, maxShardSize = null, percentageOfSourceShards = null, aliases = null, switchAliases = false, forceUnsafe = false, index = 0, targetIndexTemplate = null)
    private val shrinkStepWithoutAliasesSwitch = WaitForShrinkStep(shrinkActionWithoutAliasesSwitch)

    private val shrinkAction = ShrinkAction(numNewShards = 1, maxShardSize = null, percentageOfSourceShards = null, aliases = null, switchAliases = true, forceUnsafe = false, index = 0, targetIndexTemplate = null)
    private val waitForShrinkStep = WaitForShrinkStep(shrinkAction)
    private val managedIndexMetaData = ManagedIndexMetaData("source_index_name", "indexUuid", "policy_id", null, null, null, null, null, null, null, null, null, null, null)
    private val shrinkActionProperties = ShrinkActionProperties("node_name", "target_index_name", 1, 1234L, 1234L, 1234567L, 100000L, emptyMap())

    fun `test switchAliases should succeed and move no aliases given aliases switch is disabled`() {
        val client = getClient(getAdminClient(getIndicesAdminClient(ackedResponse, null)))
        val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)

        runBlocking {
            val aliasesSwitched = shrinkStepWithoutAliasesSwitch.switchAliases(context, shrinkActionProperties)
            assertTrue(aliasesSwitched)
        }

        verify(client.admin().indices(), never()).aliases(any(), any())
    }

    fun `test switchAliases should move all aliases from a source index to a target index`() {
        val client = getClient(getAdminClient(getIndicesAdminClient(ackedResponse, null)))
        val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)

        val targetIndexMetadata: IndexMetadata =
            mock {
                on { aliases } doReturn mapOf("target-alias" to AliasMetadata.builder("target-alias").build())
            }
        whenever(metadata.index("target_index_name")).doReturn(targetIndexMetadata)

        val sourceIndexMetadata: IndexMetadata =
            mock {
                on { aliases } doReturn mapOf("source-alias" to AliasMetadata.builder("source-alias").build())
            }
        whenever(metadata.index("source_index_name")).doReturn(sourceIndexMetadata)

        runBlocking {
            val aliasesSwitched = waitForShrinkStep.switchAliases(context, shrinkActionProperties)
            assertTrue(aliasesSwitched)
        }

        val argMatcher =
            ArgumentMatcher { request: IndicesAliasesRequest ->
                val addToTarget =
                    request.aliasActions
                        .filter { it.actionType() == IndicesAliasesRequest.AliasActions.Type.ADD }
                        .filter { it.indices().contentEquals(arrayOf("target_index_name")) }
                        .filter { it.aliases().contentEquals(arrayOf("source-alias")) }
                        .size == 1
                val removeFromSource =
                    request.aliasActions
                        .filter { it.actionType() == IndicesAliasesRequest.AliasActions.Type.REMOVE }
                        .filter { it.indices().contentEquals(arrayOf("source_index_name")) }
                        .filter { it.aliases().contentEquals(arrayOf("source-alias")) }
                        .size == 1
                val onlyTwoActions = request.aliasActions.size == 2
                addToTarget && removeFromSource && onlyTwoActions
            }
        verify(client.admin().indices()).aliases(argThat(argMatcher), any())
    }

    fun `test switchAliases should give precedence to an alias in a target index if there is a name conflict with source index alias`() {
        val client = getClient(getAdminClient(getIndicesAdminClient(ackedResponse, null)))
        val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)

        val targetIndexMetadata: IndexMetadata =
            mock {
                on { aliases } doReturn mapOf("conflict-alias" to AliasMetadata.builder("conflict-alias").build())
            }
        whenever(metadata.index("target_index_name")).doReturn(targetIndexMetadata)

        val sourceIndexMetadata: IndexMetadata =
            mock {
                on { aliases } doReturn mapOf("conflict-alias" to AliasMetadata.builder("conflict-alias").build())
            }
        whenever(metadata.index("source_index_name")).doReturn(sourceIndexMetadata)

        runBlocking {
            val aliasesSwitched = waitForShrinkStep.switchAliases(context, shrinkActionProperties)
            assertTrue(aliasesSwitched)
        }

        val argMatcher =
            ArgumentMatcher { request: IndicesAliasesRequest ->

                val removeFromSource =
                    request.aliasActions
                        .filter { it.actionType() == IndicesAliasesRequest.AliasActions.Type.REMOVE }
                        .filter { it.indices().contentEquals(arrayOf("source_index_name")) }
                        .filter { it.aliases().contentEquals(arrayOf("conflict-alias")) }
                        .size == 1
                val onlyOneAction = request.aliasActions.size == 1
                removeFromSource && onlyOneAction
            }
        verify(client.admin().indices()).aliases(argThat(argMatcher), any())
    }

    fun `test switchAliases should fail given exception is thrown while executing aliases request`() {
        val client = getClient(getAdminClient(getIndicesAdminClient(null, Exception())))
        val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)

        val targetIndexMetadata: IndexMetadata =
            mock {
                on { aliases } doReturn emptyMap()
            }
        whenever(metadata.index("target_index_name")).doReturn(targetIndexMetadata)

        val sourceIndexMetadata: IndexMetadata =
            mock {
                on { aliases } doReturn emptyMap()
            }
        whenever(metadata.index("source_index_name")).doReturn(sourceIndexMetadata)

        runBlocking {
            val aliasesSwitched = waitForShrinkStep.switchAliases(context, shrinkActionProperties)
            assertFalse(aliasesSwitched)
        }
    }

    fun `test switchAliases should fail given aliases request is not acknowledged`() {
        val client = getClient(getAdminClient(getIndicesAdminClient(unAckedResponse, null)))
        val context = StepContext(managedIndexMetaData, clusterService, client, null, null, scriptService, settings, lockService)

        val targetIndexMetadata: IndexMetadata =
            mock {
                on { aliases } doReturn emptyMap()
            }
        whenever(metadata.index("target_index_name")).doReturn(targetIndexMetadata)

        val sourceIndexMetadata: IndexMetadata =
            mock {
                on { aliases } doReturn emptyMap()
            }
        whenever(metadata.index("source_index_name")).doReturn(sourceIndexMetadata)

        runBlocking {
            val aliasesSwitched = waitForShrinkStep.switchAliases(context, shrinkActionProperties)
            assertFalse(aliasesSwitched)
        }
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }

    private fun getIndicesAdminClient(response: AcknowledgedResponse?, exception: Exception?): IndicesAdminClient {
        assertTrue("Must provide one and only one response or exception", (response != null).xor(exception != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<AcknowledgedResponse>>(1)
                if (response != null) {
                    listener.onResponse(response)
                } else {
                    listener.onFailure(exception)
                }
            }.whenever(this.mock).aliases(any(), any())
        }
    }
}
