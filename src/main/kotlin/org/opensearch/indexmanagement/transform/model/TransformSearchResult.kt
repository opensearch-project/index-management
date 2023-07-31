/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.action.index.IndexRequest
import org.opensearch.index.shard.ShardId

data class TransformSearchResult(val stats: TransformStats, val docsToIndex: List<IndexRequest>, val afterKey: Map<String, Any>? = null)

/**
 * Only used by continuous transform, to keep track of
 * - shards with new documents
 * - buckets being processed
 * in one job run
 *
 * @param modifiedBuckets set of bucket, each bucket is represented by its key
 *  which is a map of group name and value
 */
data class BucketsToTransform(
    val modifiedBuckets: MutableSet<Map<String, Any>>,
    val metadata: TransformMetadata,
    val shardsToSearch: Iterator<ShardWithNewDocuments>? = null,
    val currentShardToSearch: ShardWithNewDocuments? = null,
)

/**
 * Initializes the shards to search in this job run of continuous transform
 */
fun BucketsToTransform.initializeShardsToSearch(
    previousGlobalCheckpoints: Map<ShardId, Long>?,
    currentGlobalCheckpoints: Map<ShardId, Long>
): BucketsToTransform {
    val shardsToSearch = getShardsToSearch(previousGlobalCheckpoints, currentGlobalCheckpoints).iterator()
    return this.copy(
        shardsToSearch = shardsToSearch,
        currentShardToSearch = if (shardsToSearch.hasNext()) shardsToSearch.next() else null
    )
}

/**
 * Processes through the old and new maps of global checkpoints to generate a list of shards to search
 */
private fun getShardsToSearch(prevCheckpoints: Map<ShardId, Long>?, currCheckpoints: Map<ShardId, Long>): List<ShardWithNewDocuments> {
    val shardsToSearch: MutableList<ShardWithNewDocuments> = ArrayList()
    currCheckpoints.forEach { (shardId, currentMaxSeqNo) ->
        // if there are no seq number records, or no records for this shard, or the shard seq number is greater than the record, search it
        if ((prevCheckpoints == null) || prevCheckpoints[shardId].let { it == null || currentMaxSeqNo > it }) {
            shardsToSearch.add(ShardWithNewDocuments(shardId, prevCheckpoints?.get(shardId), currentMaxSeqNo))
        }
    }
    return shardsToSearch
}

data class BucketSearchResult(
    val modifiedBuckets: MutableSet<Map<String, Any>>,
    val afterKey: Map<String, Any>? = null,
    val searchTimeInMillis: Long = 0
)

/**
 * Holds the shardId and the seq number range to search the documents
 */
data class ShardWithNewDocuments(val shardId: ShardId, val from: Long?, val to: Long)
