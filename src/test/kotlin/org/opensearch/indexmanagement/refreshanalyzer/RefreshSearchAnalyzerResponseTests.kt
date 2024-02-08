/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.refreshanalyzer

import org.junit.Assert
import org.opensearch.core.action.support.DefaultShardOperationFailedException
import org.opensearch.core.index.shard.ShardId
import org.opensearch.test.OpenSearchTestCase

class RefreshSearchAnalyzerResponseTests : OpenSearchTestCase() {
    fun `test get successful refresh details`() {
        val index1 = "index1"
        val index2 = "index2"
        val syn1 = "synonym1"
        val syn2 = "synonym2"
        val i1s0 = ShardId(index1, "abc", 0)
        val i1s1 = ShardId(index1, "abc", 1)
        val i2s0 = ShardId(index2, "xyz", 0)
        val i2s1 = ShardId(index2, "xyz", 1)

        val responseI1s0 = RefreshSearchAnalyzerShardResponse(i1s0, listOf(syn1, syn2))
        val responseI1s1 = RefreshSearchAnalyzerShardResponse(i1s1, listOf(syn1, syn2))
        val responseI2s0 = RefreshSearchAnalyzerShardResponse(i2s0, listOf(syn1))
        val responseI2s1 = RefreshSearchAnalyzerShardResponse(i2s1, listOf(syn1))
        val failureI1s0 = DefaultShardOperationFailedException(index1, 0, Throwable("dummyCause"))
        val failureI1s1 = DefaultShardOperationFailedException(index1, 1, Throwable("dummyCause"))
        val failureI2s0 = DefaultShardOperationFailedException(index2, 0, Throwable("dummyCause"))
        val failureI2s1 = DefaultShardOperationFailedException(index2, 1, Throwable("dummyCause"))

        // Case 1: All shards successful
        var aggregateResponse = listOf(responseI1s0, responseI1s1, responseI2s0, responseI2s1)
        var aggregateFailures = listOf<DefaultShardOperationFailedException>()
        var refreshSearchAnalyzerResponse = RefreshSearchAnalyzerResponse(4, 4, 0, aggregateFailures, aggregateResponse)
        var successfulIndices = refreshSearchAnalyzerResponse.getSuccessfulRefreshDetails()
        Assert.assertTrue(successfulIndices.containsKey(index1))
        Assert.assertTrue(successfulIndices.containsKey(index2))

        // Case 2: All shards failed
        aggregateResponse = listOf<RefreshSearchAnalyzerShardResponse>()
        aggregateFailures = listOf(failureI1s0, failureI1s1, failureI2s0, failureI2s1)
        refreshSearchAnalyzerResponse = RefreshSearchAnalyzerResponse(4, 0, 4, aggregateFailures, aggregateResponse)
        successfulIndices = refreshSearchAnalyzerResponse.getSuccessfulRefreshDetails()
        Assert.assertTrue(successfulIndices.isEmpty())

        // Case 3: Some shards of an index fail, while some others succeed
        aggregateResponse = listOf(responseI1s1, responseI2s0, responseI2s1)
        aggregateFailures = listOf(failureI1s0)
        refreshSearchAnalyzerResponse = RefreshSearchAnalyzerResponse(4, 3, 1, aggregateFailures, aggregateResponse)
        successfulIndices = refreshSearchAnalyzerResponse.getSuccessfulRefreshDetails()
        Assert.assertTrue(successfulIndices.containsKey(index2))
        Assert.assertFalse(successfulIndices.containsKey(index1))
    }
}
