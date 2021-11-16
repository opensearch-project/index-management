/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.refreshanalyzer

import org.junit.Assert
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.index.Index
import org.opensearch.index.shard.ShardId
import org.opensearch.test.OpenSearchTestCase

class RefreshSearchAnalyzerShardResponseTests : OpenSearchTestCase() {

    fun `test shard refresh response parsing`() {
        val reloadedAnalyzers = listOf("analyzer1", "analyzer2")
        val refreshShardResponse = RefreshSearchAnalyzerShardResponse(ShardId(Index("testIndex", "qwerty"), 0), reloadedAnalyzers)

        val refreshShardResponse2 = roundTripRequest(refreshShardResponse)
        Assert.assertEquals(refreshShardResponse2.shardId, refreshShardResponse.shardId)
    }

    @Throws(Exception::class)
    private fun roundTripRequest(response: RefreshSearchAnalyzerShardResponse): RefreshSearchAnalyzerShardResponse {
        BytesStreamOutput().use { out ->
            response.writeTo(out)
            out.bytes().streamInput().use { si -> return RefreshSearchAnalyzerShardResponse(si) }
        }
    }
}
