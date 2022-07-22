/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.coordinator

import org.junit.Before
import org.mockito.Mockito
import org.opensearch.action.admin.cluster.node.info.NodesInfoAction
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.OpenSearchAllocationTestCase
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.SkipExecution
import org.opensearch.threadpool.ThreadPool

class SkipExecutionTests : OpenSearchAllocationTestCase() {

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var settings: Settings
    private lateinit var threadPool: ThreadPool
    private lateinit var skip: SkipExecution

    @Before
    @Throws(Exception::class)
    fun setup() {
        client = Mockito.mock(Client::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        settings = Mockito.mock(Settings::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        skip = SkipExecution(settings, threadPool, client, clusterService)
    }

    fun `test cluster change event`() {
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        Mockito.`when`(event.nodesChanged()).thenReturn(true)
        skip.clusterChanged(event)
        Mockito.verify(client).execute(Mockito.eq(NodesInfoAction.INSTANCE), Mockito.any(), Mockito.any())
    }
}
