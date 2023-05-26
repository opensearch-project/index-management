/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.junit.BeforeClass
import org.mockito.Mockito
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.test.OpenSearchTestCase

open class BaseRespParserTests : OpenSearchTestCase() {
    companion object {
        lateinit var clusterService: ClusterService

        @JvmStatic
        @BeforeClass
        fun setupClusterService() {
            val node: DiscoveryNode = Mockito.mock()
            Mockito.`when`(node.id).thenReturn("mJzoy8SBuTW12rbV8jSg")
            val clusterState: ClusterState = Mockito.mock()
            clusterService = Mockito.mock()

            Mockito.`when`(clusterService.clusterName).thenReturn(ClusterName("test-cluster"))
            Mockito.`when`(clusterService.localNode()).thenReturn(node)
            Mockito.`when`(clusterService.state()).thenReturn(clusterState)
        }
    }
}
