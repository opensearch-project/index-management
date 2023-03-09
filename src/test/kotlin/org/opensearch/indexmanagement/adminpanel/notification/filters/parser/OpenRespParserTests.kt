/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filters.parser

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.times
import org.junit.Assert
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.action.IndicesRequest
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.admin.indices.open.OpenIndexResponse
import org.opensearch.action.support.ActiveShardCount
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.adminpanel.notification.filter.parser.OpenRespParser
import org.opensearch.test.OpenSearchTestCase
import java.lang.Exception

class OpenRespParserTests : OpenSearchTestCase() {

    private lateinit var activeShardsObserver: ActiveShardsObserver
    private lateinit var clusterService: ClusterService
    private lateinit var indexNameExpressionResolver: IndexNameExpressionResolver
    private lateinit var clusterState: ClusterState

    @Before
    fun setup() {
        activeShardsObserver = Mockito.mock(ActiveShardsObserver::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        clusterState = Mockito.mock(ClusterState::class.java)
        Mockito.`when`(clusterService.state()).thenReturn(clusterState)
        indexNameExpressionResolver = Mockito.mock(IndexNameExpressionResolver::class.java)
    }

    fun `test all shards are started`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val response = OpenIndexResponse(true, true)
        val parser = OpenRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        parser.parseAndSendNotification(response) {
            Assert.assertEquals(it, "open index [index-1,index-2] has completed.")
        }

        Mockito.verify(activeShardsObserver, never())
            .waitForActiveShards(any(), any(), any(), any(), any())
    }

    fun `test not all shards are started`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val response = OpenIndexResponse(true, false)
        val parser = OpenRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        doReturn(arrayOf("index-1", "index-2"))
            .`when`(indexNameExpressionResolver).concreteIndexNames(any(), any() as IndicesRequest)

        parser.parseAndSendNotification(response) {
            Assert.assertEquals(it, "open index [index-1,index-2] has completed.")
        }

        Mockito.verify(activeShardsObserver, times(1))
            .waitForActiveShards(any(), Mockito.eq(ActiveShardCount.DEFAULT), any(), any(), any())
    }

    fun `test build message for completion`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val response = OpenIndexResponse(true, true)
        val parser = OpenRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(msg, "open index [index-1,index-2] has completed.")
    }

    fun `test build message for failure`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val response = OpenIndexResponse(true, true)
        val parser = OpenRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        val msg = parser.buildNotificationMessage(response, Exception("index already exits error"))
        Assert.assertEquals(
            msg,
            "open index [index-1,index-2] has completed with errors. Error details: index already exits error"
        )
    }

    fun `test build message for timeout`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val response = OpenIndexResponse(true, true)
        val parser = OpenRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        val msg = parser.buildNotificationMessage(response, isTimeout = true)
        Assert.assertEquals(msg, "open index [index-1,index-2] has timeout within 12h.")
    }
}
