/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.ClusterAdminClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.ImmutableOpenMap
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class MetadataServiceTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val clusterState: ClusterState = mock()
    private val metadata: Metadata = mock()
    private val imIndices: IndexManagementIndices = mock()

    private val ex = Exception()

    @Before
    fun setup() {
        whenever(clusterService.state()).doReturn(clusterState)
        whenever(clusterState.metadata).doReturn(metadata)
        whenever(metadata.indices).doReturn(ImmutableOpenMap.of())
    }

    fun `test config index not exists`() = runBlocking {
        whenever(imIndices.indexManagementIndexExists()).doReturn(false)

        val client = getClient(
            getAdminClient(
                getClusterAdminClient(
                    updateSettingResponse = null,
                    updateSettingException = ex
                )
            )
        )
        val skipFlag = SkipExecution(client, clusterService)
        val metadataService = MetadataService(client, clusterService, skipFlag, imIndices)
        metadataService.moveMetadata()

        verify(client.admin().cluster(), never()).updateSettings(any(), any())
        assertEquals(metadataService.finishFlag, true)
    }

    // If update setting to 1 failed with some exception, runTimeCounter shouldn't be increased
    fun `test failed to update setting to 1`() = runBlocking {
        whenever(imIndices.indexManagementIndexExists()).doReturn(true)

        val client = getClient(
            getAdminClient(
                getClusterAdminClient(
                    updateSettingResponse = null,
                    updateSettingException = ex
                )
            )
        )

        val skipFlag = SkipExecution(client, clusterService)
        val metadataService = MetadataService(client, clusterService, skipFlag, imIndices)
        metadataService.moveMetadata()
        assertEquals(metadataService.runTimeCounter, 2)
        metadataService.moveMetadata()
        assertEquals(metadataService.runTimeCounter, 3)
        metadataService.moveMetadata()
        assertEquals(metadataService.runTimeCounter, 4)
        assertFailsWith(Exception::class) {
            runBlocking {
                metadataService.moveMetadata()
            }
        }
        assertEquals(metadataService.runTimeCounter, 4)
        assertEquals(metadataService.finishFlag, false)
    }

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(clusterAdminClient: ClusterAdminClient): AdminClient = mock { on { cluster() } doReturn clusterAdminClient }

    private fun getClusterAdminClient(
        updateSettingResponse: ClusterUpdateSettingsResponse?,
        updateSettingException: Exception?
    ): ClusterAdminClient {
        assertTrue(
            "Must provide either a getMappingsResponse or getMappingsException",
            (updateSettingResponse != null).xor(updateSettingException != null)
        )

        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<ClusterUpdateSettingsResponse>>(1)
                if (updateSettingResponse != null) listener.onResponse(updateSettingResponse)
                else listener.onFailure(updateSettingException)
            }.whenever(this.mock).updateSettings(any(), any())
        }
    }
}
