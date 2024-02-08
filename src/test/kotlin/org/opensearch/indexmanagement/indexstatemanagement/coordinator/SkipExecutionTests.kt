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
import org.opensearch.indexmanagement.indexstatemanagement.SkipExecution

class SkipExecutionTests : OpenSearchAllocationTestCase() {
    private lateinit var client: Client
    private lateinit var skip: SkipExecution

    @Before
    @Throws(Exception::class)
    fun setup() {
        client = Mockito.mock(Client::class.java)
        skip = SkipExecution(client)
    }

    fun `test cluster change event`() {
        val event = Mockito.mock(ClusterChangedEvent::class.java)
        Mockito.`when`(event.nodesChanged()).thenReturn(true)
        skip.sweepISMPluginVersion()
        Mockito.verify(client).execute(Mockito.eq(NodesInfoAction.INSTANCE), Mockito.any(), Mockito.any())
    }
}
