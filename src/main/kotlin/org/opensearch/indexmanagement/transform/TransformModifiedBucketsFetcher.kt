/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.search.MultiSearchResponse
import org.opensearch.client.Client
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.transform.model.ShardNewDocuments
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.util.TransformContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

class TransformModifiedBucketsFetcher(
    val client: Client,
    var shardNewDocumentsList: MutableList<ShardNewDocuments>,
    val transform: Transform,
    var transformMetadata: TransformMetadata,
    val transformContext: TransformContext,
    val transformSearchService: TransformSearchService,
    val transformMetadataService: TransformMetadataService
) : CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("TransformModifiedBucketsFetcher")) {

    private val transformBucketsStore = TransformBucketsStore()

    private var logger = LogManager.getLogger(javaClass)

    var afterKeyList = ArrayList<Map<String, Any>?>(shardNewDocumentsList.size)

    init {
        shardNewDocumentsList.forEach { afterKeyList.add(null) }
    }

    suspend fun fetchAllModifiedBuckets(): Triple<List<Map<String, Any>>, List<ShardId>, TransformMetadata> {
        val failedShards = mutableListOf<ShardId>()
        do {
            val multiSearchRequestBuilder = transformSearchService.createMultiSearchRequestBuilder(transform, shardNewDocumentsList, transformContext, afterKeyList)
            if (multiSearchRequestBuilder.request().requests().size == 0) {
                return Triple(listOf(), listOf(), transformMetadata)
            }
            val multiSearchResponse: MultiSearchResponse = suspendCoroutine { cont ->
                multiSearchRequestBuilder.execute(
                    object : ActionListener<MultiSearchResponse> {
                        override fun onResponse(response: MultiSearchResponse) = cont.resume(response)
                        override fun onFailure(t: Exception) = cont.resumeWithException(t)
                    }
                )
            }
            multiSearchResponse.responses.forEachIndexed { index, resp ->
                if (resp.isFailure) {
                    logger.error("Failed fetching modified buckets from shard:${shardNewDocumentsList[index].shardId.id}: ${resp.failureMessage}")
                    afterKeyList[index] = null
                    failedShards.add(shardNewDocumentsList[index].shardId)
                } else {
                    processSearchResult(index, resp)
                }
            }

            val newAfterKeyList = ArrayList<Map<String, Any>?>()
            val newShardNewDocumentsList = ArrayList<ShardNewDocuments>()
            afterKeyList.forEachIndexed { index, afterKey ->
                if (afterKey != null) {
                    newAfterKeyList.add(afterKey)
                    newShardNewDocumentsList.add(shardNewDocumentsList[index])
                }
            }
            afterKeyList = newAfterKeyList
            shardNewDocumentsList = newShardNewDocumentsList
        } while (shardNewDocumentsList.size > 0)

        val updatedMetadata = transformMetadataService.writeMetadata(transformMetadata, true)

        return Triple(transformBucketsStore.getAllBuckets(), failedShards, updatedMetadata)
    }

    private suspend fun processSearchResult(index: Int, response: MultiSearchResponse.Item) {
        val transformSearchResult = TransformSearchService.convertBucketSearchResponse(transform, response.response!!)
        // add new buckets to bucket store
        transformBucketsStore.addBuckets(transformSearchResult.modifiedBuckets)
        // update metadata - merge stats: searchTimeInMillis and increment pagesProcessed
        val mergedSearchTime = transformMetadata.stats.searchTimeInMillis +
            transformSearchResult.searchTimeInMillis

        transformMetadata = transformMetadata.copy(
            stats = transformMetadata.stats.copy(
                pagesProcessed = transformMetadata.stats.pagesProcessed + 1,
                searchTimeInMillis = mergedSearchTime
            )
        )
        afterKeyList[index] = transformSearchResult.afterKey
    }
}
