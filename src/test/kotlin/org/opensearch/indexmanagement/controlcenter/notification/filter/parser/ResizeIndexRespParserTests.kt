/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.times
import org.junit.Assert
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.admin.indices.shrink.ResizeResponse
import org.opensearch.action.admin.indices.shrink.ResizeType
import org.opensearch.action.support.ActiveShardCount
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.index.Index
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import java.lang.IllegalStateException

class ResizeIndexRespParserTests : BaseRespParserTests() {

    private lateinit var activeShardsObserver: ActiveShardsObserver

    @Before
    fun setup() {
        activeShardsObserver = Mockito.mock(ActiveShardsObserver::class.java)
    }

    fun `test all shards are started`() {
        val request = ResizeRequest("target", "source")
        request.resizeType = ResizeType.SHRINK
        val response = ResizeResponse(true, true, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        parser.parseAndSendNotification(response) { ret ->
            Assert.assertEquals(ret.message, "The shrink operation from [test-cluster/source] to [test-cluster/target] has been completed.")
            Assert.assertEquals(ret.title, "Shrink operation on [test-cluster/source] has completed")
        }

        Mockito.verify(activeShardsObserver, never())
            .waitForActiveShards(any(), any(), any(), any(), any())
    }

    fun `test not all shards are started sync`() {
        val request = ResizeRequest("target", "source")
        request.resizeType = ResizeType.SHRINK
        val response = ResizeResponse(true, false, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        parser.parseAndSendNotification(response) {}

        Mockito.verify(activeShardsObserver, times(1))
            .waitForActiveShards(any(), Mockito.eq(ActiveShardCount.DEFAULT), any(), any(), any())
    }

    fun `test not all shards are started async`() {
        val request = ResizeRequest("target", "source")
        request.resizeType = ResizeType.SHRINK
        request.targetIndexRequest.timeout(TimeValue.timeValueMinutes(10))
        val response = ResizeResponse(true, false, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        parser.parseAndSendNotification(response) {}

        Mockito.verify(activeShardsObserver, times(1))
            .waitForActiveShards(
                any(),
                Mockito.eq(ActiveShardCount.DEFAULT),
                eq(TimeValue.timeValueMinutes(50)),
                any(),
                any()
            )
    }
    fun `test source index not exist exception`() {
        val request = ResizeRequest("target", "source_index")
        request.resizeType = ResizeType.SHRINK
        request.targetIndexRequest.timeout(TimeValue.timeValueMinutes(10))
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        parser.parseAndSendNotification(null, IndexNotFoundException("source_index")) { ret ->
            Assert.assertEquals(ret.operationResult, OperationResult.FAILED)
            Assert.assertEquals(ret.message, "The [test-cluster/source_index] does not exist.")
            Assert.assertEquals(ret.title, "Shrink operation on [test-cluster/source_index] has failed")
        }
    }

    fun `test block write has not set exception`() {
        val request = ResizeRequest("target", "source_index")
        request.resizeType = ResizeType.SHRINK
        request.targetIndexRequest.timeout(TimeValue.timeValueMinutes(10))
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        parser.parseAndSendNotification(null, IllegalStateException("index source_index must block write operations to resize index. use \"index.blocks.write=true\"")) { ret ->
            Assert.assertEquals(ret.operationResult, OperationResult.FAILED)
            Assert.assertEquals(ret.message, "[test-cluster/source_index] must be set to block write to shrink the index. To set it to block write, use `PUT /source_index/_block/write` ")
            Assert.assertEquals(ret.title, "Shrink operation on [test-cluster/source_index] has failed")
        }
    }

    fun `test shards not in same node exception`() {
        val request = ResizeRequest("target", "source_index")
        request.resizeType = ResizeType.SPLIT
        request.targetIndexRequest.timeout(TimeValue.timeValueMinutes(10))
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        parser.parseAndSendNotification(null, IllegalStateException("index source_index must have all shards allocated on the same node to shrink index")) { ret ->
            Assert.assertEquals(ret.operationResult, OperationResult.FAILED)
            Assert.assertEquals(
                ret.message,
                "You must allocate a copy of every shard of the source index to the same node before split. To allocate it to same node, try use PUT /source_index/_settings\n" +
                    "{\n" +
                    "\"index.routing.allocation.require._name\":\"your_node_name\"\n" +
                    "}"
            )
            Assert.assertEquals(ret.title, "Split operation on [test-cluster/source_index] has failed")
        }
    }
    fun `test not all shards are started timeout`() {
        val request = ResizeRequest("target", "source")
        request.resizeType = ResizeType.SHRINK
        request.targetIndexRequest.timeout(TimeValue.timeValueHours(4))
        val response = ResizeResponse(true, false, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        parser.parseAndSendNotification(response) { ret ->
            Assert.assertEquals(
                ret.message,
                "The shrink operation from [test-cluster/source] to [test-cluster/target] has taken more than 4h to complete. To see the latest status, use `GET /target/_recovery`"
            )
            Assert.assertEquals(ret.title, "Shrink operation on [test-cluster/source] has timed out")
        }

        Mockito.verify(activeShardsObserver, never())
            .waitForActiveShards(any(), Mockito.eq(ActiveShardCount.DEFAULT), any(), any(), any())
    }

    fun `test build message for completion`() {
        val request = ResizeRequest("target", "source")
        request.resizeType = ResizeType.SHRINK
        val response = ResizeResponse(true, false, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        val msg = parser.buildNotificationMessage(response)
        Assert.assertEquals(msg, "The shrink operation from [test-cluster/source] to [test-cluster/target] has been completed.")

        val title = parser.buildNotificationTitle(OperationResult.COMPLETE)
        Assert.assertEquals(title, "Shrink operation on [test-cluster/source] has completed")
    }

    fun `test build message for failure`() {
        val request = ResizeRequest("target-index", "source")
        request.resizeType = ResizeType.CLONE
        val response = ResizeResponse(true, false, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        val msg = parser.buildNotificationMessage(response, ResourceAlreadyExistsException(Index("target-index", "uuid")))
        Assert.assertEquals(
            msg,
            "The target index [test-cluster/target-index] already exists."
        )

        val title = parser.buildNotificationTitle(OperationResult.FAILED)
        Assert.assertEquals(title, "Clone operation on [test-cluster/source] has failed")
    }

    fun `test build message for timeout`() {
        val request = ResizeRequest("target", "source")
        request.resizeType = ResizeType.SPLIT
        val response = ResizeResponse(true, false, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        val msg = parser.buildNotificationMessage(response, isTimeout = true)
        Assert.assertEquals(
            msg,
            "The split operation from [test-cluster/source] to [test-cluster/target] has taken more than 1h to complete. To see the latest status, use `GET /target/_recovery`"
        )

        val title = parser.buildNotificationTitle(OperationResult.TIMEOUT)
        Assert.assertEquals(title, "Split operation on [test-cluster/source] has timed out")
    }
}
