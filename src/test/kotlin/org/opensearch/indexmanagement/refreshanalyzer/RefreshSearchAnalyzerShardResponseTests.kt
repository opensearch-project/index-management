/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
