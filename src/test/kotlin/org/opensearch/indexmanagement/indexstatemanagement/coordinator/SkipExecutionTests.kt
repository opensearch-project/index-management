/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.coordinator

import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.junit.Before
import org.opensearch.Version
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.service.ClusterService
import org.opensearch.core.common.transport.TransportAddress
import org.opensearch.indexmanagement.indexstatemanagement.SkipExecution
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Ignore

class SkipExecutionTests : OpenSearchTestCase() {
    private var clusterService: ClusterService = mock()
    private lateinit var clusterState: ClusterState
    private lateinit var skip: SkipExecution

    @Before
    fun setup() {
        skip = SkipExecution()
    }

    fun `test sweepISMPluginVersion should set flag to false and hasLegacyPlugin to false when all nodes have the same version`() {
        val version = Version.CURRENT
        val node1 = DiscoveryNode("node1", TransportAddress(TransportAddress.META_ADDRESS, 9300), version)
        val node2 = DiscoveryNode("node2", TransportAddress(TransportAddress.META_ADDRESS, 9301), version)
        val discoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).build()
        clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoveryNodes).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        skip.sweepISMPluginVersion(clusterService)

        assertFalse(skip.flag)
        assertFalse(skip.hasLegacyPlugin)
    }

    fun `test sweepISMPluginVersion should set flag to true and hasLegacyPlugin to false when all nodes have the different versions`() {
        val version1 = Version.CURRENT
        val version2 = Version.V_2_0_0
        val node1 = DiscoveryNode("node1", TransportAddress(TransportAddress.META_ADDRESS, 9300), version1)
        val node2 = DiscoveryNode("node2", TransportAddress(TransportAddress.META_ADDRESS, 9301), version2)
        val node3 = DiscoveryNode("node3", TransportAddress(TransportAddress.META_ADDRESS, 9302), version2)
        val discoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).add(node3).build()
        clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoveryNodes).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        skip.sweepISMPluginVersion(clusterService)

        assertTrue(skip.flag)
        assertFalse(skip.hasLegacyPlugin)
    }

    @Ignore("Legacy version was remove https://github.com/opensearch-project/OpenSearch/pull/19793")
    fun `test sweepISMPluginVersion should set flag to true and hasLegacyPlugin to true when there are different versions including current version`() {
        val minVersion = Version.fromString("7.10.0")
        val maxVersion = Version.CURRENT
        val node1 = DiscoveryNode("node1", TransportAddress(TransportAddress.META_ADDRESS, 9300), minVersion)
        val node2 = DiscoveryNode("node2", TransportAddress(TransportAddress.META_ADDRESS, 9301), maxVersion)
        val node3 = DiscoveryNode("node3", TransportAddress(TransportAddress.META_ADDRESS, 9302), maxVersion)
        val discoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).add(node3).build()
        clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoveryNodes).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        skip.sweepISMPluginVersion(clusterService)

        assertTrue(skip.flag)
        assertTrue(skip.hasLegacyPlugin)
    }

    @Ignore("Legacy version was remove https://github.com/opensearch-project/OpenSearch/pull/19793")
    fun `test sweepISMPluginVersion should set flag to true and hasLegacyPlugin to true with different versions`() {
        val minVersion = Version.fromString("7.10.0")
        val maxVersion = Version.V_2_0_0
        val node1 = DiscoveryNode("node1", TransportAddress(TransportAddress.META_ADDRESS, 9300), minVersion)
        val node2 = DiscoveryNode("node2", TransportAddress(TransportAddress.META_ADDRESS, 9301), maxVersion)
        val node3 = DiscoveryNode("node3", TransportAddress(TransportAddress.META_ADDRESS, 9302), maxVersion)

        val discoveryNodes = DiscoveryNodes.builder().add(node1).add(node2).add(node3).build()
        clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoveryNodes).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        skip.sweepISMPluginVersion(clusterService)

        assertTrue(skip.flag)
        assertTrue(skip.hasLegacyPlugin)
    }
}
