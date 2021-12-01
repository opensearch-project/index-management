/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.action.index.IndexRequest
import org.opensearch.index.shard.ShardId

data class TransformSearchResult(val stats: TransformStats, val docsToIndex: List<IndexRequest>, val afterKey: Map<String, Any>? = null)

data class BucketSearchResult(
    val modifiedBuckets: MutableSet<Map<String, Any>>,
    val afterKey: Map<String, Any>? = null,
    val searchTimeInMillis: Long = 0
)

data class ShardNewDocuments(val shardId: ShardId, val from: Long?, val to: Long)
