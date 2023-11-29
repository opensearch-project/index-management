/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkItemResponse
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.transform.exceptions.TransformIndexException
import org.opensearch.indexmanagement.transform.settings.TransformSettings
import org.opensearch.indexmanagement.transform.util.TransformContext
import org.opensearch.core.rest.RestStatus
import org.opensearch.transport.RemoteTransportException
import org.opensearch.action.support.master.AcknowledgedResponse

@Suppress("ComplexMethod")
class TransformIndexer(
    settings: Settings,
    private val clusterService: ClusterService,
    private val client: Client
) {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile
    private var backoffPolicy = BackoffPolicy.constantBackoff(
        TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_MILLIS.get(settings),
        TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_COUNT.get(settings)
    )

    init {
        // To update the retry policy with updated settings
        clusterService.clusterSettings.addSettingsUpdateConsumer(
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_MILLIS,
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_COUNT
        ) { millis, count ->
            backoffPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    private suspend fun createTargetIndex(targetIndex: String, targetFieldMappings: Map<String, Any>) {
        if (!clusterService.state().routingTable.hasIndex(targetIndex)) {
            val transformTargetIndexMapping = TargetIndexMappingService.createTargetIndexMapping(targetFieldMappings)
            val request = CreateIndexRequest(targetIndex).mapping(transformTargetIndexMapping)
            // TODO: Read in the actual mappings from the source index and use that
            val response: CreateIndexResponse = client.admin().indices().suspendUntil { create(request, it) }
            if (!response.isAcknowledged) {
                logger.error("Failed to create the target index $targetIndex")
                throw TransformIndexException("Failed to create the target index")
            }
        }
        val writeIndexMetadata = clusterService.state().metadata.indicesLookup[targetIndex]!!.writeIndex
        if (clusterService.state().metadata.hasAlias(targetIndex)) {
            // return error if no write index with the alias
            if (writeIndexMetadata == null) {
                throw TransformIndexException("Target index alias has no write index")
            }
        }
        val putMappingReq = PutMappingRequest(
            writeIndexMetadata?.index?.name ?: throw TransformIndexException("Target index alias has no write index!")
        ).source(targetFieldMappings)
        val mapResp: AcknowledgedResponse = client.admin().indices().suspendUntil {
            putMapping(putMappingReq)
        }
        if (!mapResp.isAcknowledged) {
            logger.error("Target index mapping request failed")
        }
    }

    @Suppress("ThrowsCount", "RethrowCaughtException")
    suspend fun index(transformTargetIndex: String, docsToIndex: List<DocWriteRequest<*>>, transformContext: TransformContext): Long {
        var updatableDocsToIndex = docsToIndex
        var indexTimeInMillis = 0L
        val nonRetryableFailures = mutableListOf<BulkItemResponse>()
        try {
            if (updatableDocsToIndex.isNotEmpty()) {
                val targetIndex = updatableDocsToIndex.first().index()
                logger.debug("Attempting to index ${updatableDocsToIndex.size} documents to $targetIndex")

                createTargetIndex(transformTargetIndex, transformContext.getTargetIndexDateFieldMappings())
                backoffPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
                    val bulkRequest = BulkRequest().add(updatableDocsToIndex)
                    val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
                    indexTimeInMillis += bulkResponse.took.millis
                    val retryableFailures = mutableListOf<BulkItemResponse>()
                    (bulkResponse.items ?: arrayOf()).filter { it.isFailed }.forEach { failedResponse ->
                        if (failedResponse.status() == RestStatus.TOO_MANY_REQUESTS) {
                            retryableFailures.add(failedResponse)
                        } else {
                            nonRetryableFailures.add(failedResponse)
                        }
                    }
                    updatableDocsToIndex = retryableFailures.map { failure ->
                        updatableDocsToIndex[failure.itemId] as IndexRequest
                    }
                    if (updatableDocsToIndex.isNotEmpty()) {
                        throw ExceptionsHelper.convertToOpenSearchException(retryableFailures.first().failure.cause)
                    }
                }
            }
            if (nonRetryableFailures.isNotEmpty()) {
                logger.error("Failed to index ${nonRetryableFailures.size} documents")
                throw ExceptionsHelper.convertToOpenSearchException(nonRetryableFailures.first().failure.cause)
            }
            return indexTimeInMillis
        } catch (e: TransformIndexException) {
            throw e
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw TransformIndexException("Failed to index the documents", unwrappedException)
        } catch (e: OpenSearchSecurityException) {
            throw TransformIndexException("Failed to index the documents - missing required index permissions: ${e.localizedMessage}", e)
        } catch (e: Exception) {
            throw TransformIndexException("Failed to index the documents", e)
        }
    }
}
