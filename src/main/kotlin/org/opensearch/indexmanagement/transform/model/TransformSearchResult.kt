/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.action.index.IndexRequest
import org.opensearch.index.shard.ShardId

data class TransformSearchResult(val stats: TransformStats, val docsToIndex: List<IndexRequest>, val afterKey: Map<String, Any>? = null)

data class BucketsToTransform(
    val modifiedBuckets: MutableSet<Map<String, Any>>,
    val metadata: TransformMetadata,
    val shardsToSearch: Iterator<ShardNewDocuments>? = null,
    val currentShard: ShardNewDocuments? = null,
)

fun BucketsToTransform.initializeShardsToSearch(
    originalGlobalCheckpoints: Map<ShardId, Long>?,
    currentShardIdToGlobalCheckpoint: Map<ShardId, Long>
): BucketsToTransform {
    val shardsToSearch = getShardsToSearch(originalGlobalCheckpoints, currentShardIdToGlobalCheckpoint).iterator()
    return this.copy(
        shardsToSearch = shardsToSearch,
        currentShard = if (shardsToSearch.hasNext()) shardsToSearch.next() else null
    )
}

// Processes through the old and new maps of sequence numbers to generate a list of objects with the shardId and the seq numbers to search
private fun getShardsToSearch(oldShardIDToMaxSeqNo: Map<ShardId, Long>?, newShardIDToMaxSeqNo: Map<ShardId, Long>): List<ShardNewDocuments> {
    val shardsToSearch: MutableList<ShardNewDocuments> = ArrayList()
    newShardIDToMaxSeqNo.forEach { (shardId, currentMaxSeqNo) ->
        // if there are no seq number records, or no records for this shard, or the shard seq number is greater than the record, search it
        if ((oldShardIDToMaxSeqNo == null) || oldShardIDToMaxSeqNo[shardId].let { it == null || currentMaxSeqNo > it }) {
            shardsToSearch.add(ShardNewDocuments(shardId, oldShardIDToMaxSeqNo?.get(shardId), currentMaxSeqNo))
        }
    }
    return shardsToSearch
}

data class BucketSearchResult(
    val modifiedBuckets: MutableSet<Map<String, Any>>,
    val afterKey: Map<String, Any>? = null,
    val searchTimeInMillis: Long = 0
)

data class ShardNewDocuments(val shardId: ShardId, val from: Long?, val to: Long)
