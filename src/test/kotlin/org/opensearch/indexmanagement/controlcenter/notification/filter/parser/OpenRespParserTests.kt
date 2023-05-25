/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.junit.Assert
import org.junit.Before
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.opensearch.OpenSearchException
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.admin.indices.open.OpenIndexResponse
import org.opensearch.action.support.ActiveShardCount
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.Index
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import kotlin.Exception

class OpenRespParserTests : BaseRespParserTests() {

    private lateinit var activeShardsObserver: ActiveShardsObserver
    private lateinit var indexNameExpressionResolver: IndexNameExpressionResolver

    @Before
    fun setup() {
        activeShardsObserver = Mockito.mock(ActiveShardsObserver::class.java)
        indexNameExpressionResolver = Mockito.mock(IndexNameExpressionResolver::class.java)
    }

    fun `test all shards are started`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val response = OpenIndexResponse(true, true)
        val parser = OpenIndexRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        parser.parseAndSendNotification(response) { ret ->
            Assert.assertEquals(ret.message, "[index-1,index-2] from [test-cluster] have been set to open.")
            Assert.assertEquals(ret.title, "2 indexes from [test-cluster] have been opened")
        }

        Mockito.verify(activeShardsObserver, never())
            .waitForActiveShards(any(), any(), any(), any(), any())
    }

    fun `test not all shards are started sync`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val response = OpenIndexResponse(true, false)
        Mockito.`when`(indexNameExpressionResolver.concreteIndexNames(any(), any(OpenIndexRequest::class.java)))
            .thenReturn(arrayOf("index-1", "index-2"))

        val parser = OpenIndexRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        parser.parseAndSendNotification(response, null) {}

        // wait for shard to get started
        Mockito.verify(activeShardsObserver, times(1))
            .waitForActiveShards(any(), Mockito.eq(ActiveShardCount.DEFAULT), any(), any(), any())
    }

    fun `test not all shards are started async`() {
        val request: OpenIndexRequest = Mockito.mock()
        Mockito.`when`(request.indices()).thenReturn(arrayOf("index-1", "index-2"))
        Mockito.`when`(request.shouldStoreResult).thenReturn(true)
        Mockito.`when`(request.ackTimeout()).thenReturn(TimeValue.timeValueHours(1))

        val response = OpenIndexResponse(true, false)
        val parser = OpenIndexRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        Mockito
            .`when`(indexNameExpressionResolver.concreteIndexNames(any(), any(OpenIndexRequest::class.java)))
            .thenReturn(arrayOf("index-1", "index-2"))

        parser.parseAndSendNotification(response) { ret ->
            Assert.assertEquals(
                ret.message,
                "Opening the index [index-1,index-2] from [test-cluster] have taken more than 1h to complete. To see the latest status, use `GET /index-1,index-2/_recovery`",
            )
            Assert.assertEquals(ret.title, "2 indexes from [test-cluster] have timed out to open")
        }

        // don't wait for shard to get started
        Mockito.verify(activeShardsObserver, never())
            .waitForActiveShards(any(), Mockito.eq(ActiveShardCount.DEFAULT), any(), any(), any())
    }

    fun `test not all shards are started timeout`() {
        val request = OpenIndexRequest("index-1", "index-2")
        // wait for 2 hours which is greater than default wait time 1 hour
        request.timeout(TimeValue.timeValueHours(2))
        val response = OpenIndexResponse(true, false)
        val parser = OpenIndexRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        Mockito.`when`(indexNameExpressionResolver.concreteIndexNames(any(), any(OpenIndexRequest::class.java)))
            .thenReturn(arrayOf("index-1", "index-2"))

        parser.parseAndSendNotification(response, null) { ret ->
            Assert.assertEquals(ret.title, "2 indexes from [test-cluster] have timed out to open")
            Assert.assertEquals(
                ret.message,
                "Opening the index [index-1,index-2] from [test-cluster] have taken more than 2h to complete. To see the latest status, use `GET /index-1,index-2/_recovery`",
            )
        }

        Mockito.verify(activeShardsObserver, never())
            .waitForActiveShards(any(), Mockito.eq(ActiveShardCount.DEFAULT), any(), any(), any())
    }

    fun `test build message for completion`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val response = OpenIndexResponse(true, true)
        val parser = OpenIndexRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(msg, "[index-1,index-2] from [test-cluster] have been set to open.")

        val title = parser.buildNotificationTitle(OperationResult.COMPLETE)
        Assert.assertEquals(title, "2 indexes from [test-cluster] have been opened")
    }

    fun `test build message for failure`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val response = OpenIndexResponse(true, true)
        val parser = OpenIndexRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        val msg = parser.buildNotificationMessage(response, Exception("index already exits error"))
        Assert.assertEquals(
            msg,
            "index already exits error",
        )
        val title = parser.buildNotificationTitle(OperationResult.FAILED)
        Assert.assertEquals(title, "2 indexes from [test-cluster] have failed to open")
    }

    fun `test build message for exception`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val parser = OpenIndexRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        val ex = OpenSearchException("index already exits error")
        ex.index = Index("index-1", "uuid")

        parser.parseAndSendNotification(null, ex) { ret ->
            Assert.assertEquals(ret.operationResult, OperationResult.FAILED)
            Assert.assertEquals(ret.message, "index [index-1] index already exits error.")
            Assert.assertEquals(ret.title, "2 indexes from [test-cluster] have failed to open")
        }
    }

    fun `test build message for timeout`() {
        val request = OpenIndexRequest("index-1", "index-2")
        val response = OpenIndexResponse(true, true)
        val parser = OpenIndexRespParser(activeShardsObserver, request, indexNameExpressionResolver, clusterService)

        val msg = parser.buildNotificationMessage(response, isTimeout = true)
        Assert.assertEquals(
            msg,
            "Opening the index [index-1,index-2] from [test-cluster] have taken more than 1h to complete. To see the latest status, use `GET /index-1,index-2/_recovery`",
        )

        val title = parser.buildNotificationTitle(OperationResult.TIMEOUT)
        Assert.assertEquals(title, "2 indexes from [test-cluster] have timed out to open")
    }
}
