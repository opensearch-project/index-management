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
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.admin.indices.shrink.ResizeResponse
import org.opensearch.action.admin.indices.shrink.ResizeType
import org.opensearch.action.support.ActiveShardCount
import org.opensearch.action.support.ActiveShardsObserver
import org.opensearch.common.unit.TimeValue
import java.lang.Exception

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
            Assert.assertEquals(ret.message, "The shrink job on from test-cluster/source to test-cluster/target has completed.")
        }

        Mockito.verify(activeShardsObserver, never())
            .waitForActiveShards(any(), any(), any(), any(), any())
    }

    fun `test not all shards are started sync`() {
        val request = ResizeRequest("target", "source")
        request.resizeType = ResizeType.SHRINK
        val response = ResizeResponse(true, false, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        parser.parseAndSendNotification(response) { ret ->
            Assert.assertEquals(ret.message, "Shrink from source to target has completed.")
        }

        Mockito.verify(activeShardsObserver, times(1))
            .waitForActiveShards(any(), Mockito.eq(ActiveShardCount.DEFAULT), any(), any(), any())
    }

    fun `test not all shards are started async`() {
        val request = ResizeRequest("target", "source")
        request.resizeType = ResizeType.SHRINK
        request.targetIndexRequest.timeout(TimeValue.timeValueMinutes(10))
        val response = ResizeResponse(true, false, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        parser.parseAndSendNotification(response) { ret ->
            Assert.assertEquals(ret.message, "Shrink from source to target has completed.")
        }

        Mockito.verify(activeShardsObserver, times(1))
            .waitForActiveShards(
                any(),
                Mockito.eq(ActiveShardCount.DEFAULT),
                eq(TimeValue.timeValueMinutes(50)),
                any(),
                any()
            )
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
                "The shrink job on from test-cluster/source to test-cluster/target has completed, but timed out while waiting for enough shards to be started in 4h, try with `GET /target/_recovery` to get more index recovery details."
            )
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
        Assert.assertEquals(msg, "The shrink job on from test-cluster/source to test-cluster/target has completed.")
    }

    fun `test build message for failure`() {
        val request = ResizeRequest("target", "source")
        request.resizeType = ResizeType.CLONE
        val response = ResizeResponse(true, false, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        val msg = parser.buildNotificationMessage(response, Exception("index already exits error"))
        Assert.assertEquals(
            msg,
            "The clone job on from test-cluster/source to test-cluster/target has failed: index already exits error"
        )
    }

    fun `test build message for timeout`() {
        val request = ResizeRequest("target", "source")
        request.resizeType = ResizeType.SPLIT
        val response = ResizeResponse(true, false, "target")
        val parser = ResizeIndexRespParser(activeShardsObserver, request, clusterService)

        val msg = parser.buildNotificationMessage(response, isTimeout = true)
        Assert.assertEquals(
            msg,
            "The split job on from test-cluster/source to test-cluster/target has completed, but timed out while waiting for enough shards to be started in 1h, try with `GET /target/_recovery` to get more index recovery details."
        )
    }
}
