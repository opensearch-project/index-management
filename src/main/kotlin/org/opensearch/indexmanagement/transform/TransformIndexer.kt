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

package org.opensearch.indexmanagement.transform

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.transform.exceptions.TransformIndexException
import org.opensearch.indexmanagement.transform.settings.TransformSettings
import org.opensearch.indexmanagement.util._DOC
import org.opensearch.rest.RestStatus
import org.opensearch.transport.RemoteTransportException

class TransformIndexer(
    settings: Settings,
    private val clusterService: ClusterService,
    private val client: Client
) {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var backoffPolicy = BackoffPolicy.constantBackoff(
        TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_MILLIS.get(settings),
        TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_COUNT.get(settings)
    )

    init {
        // To update the retry policy with updated settings
        clusterService.clusterSettings.addSettingsUpdateConsumer(
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_MILLIS,
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_COUNT
        ) {
            millis, count ->
            backoffPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    private suspend fun createTargetIndex(index: String) {
        if (!clusterService.state().routingTable.hasIndex(index)) {
            val request = CreateIndexRequest(index)
                .mapping(_DOC, IndexManagementIndices.transformTargetMappings, XContentType.JSON)
            // TODO: Read in the actual mappings from the source index and use that
            val response: CreateIndexResponse = client.admin().indices().suspendUntil { create(request, it) }
            if (!response.isAcknowledged) {
                logger.error("Failed to create the target index $index")
                throw TransformIndexException("Failed to create the target index")
            }
        }
    }

    @Suppress("ThrowsCount")
    suspend fun index(docsToIndex: List<DocWriteRequest<*>>): Long {
        var updatableDocsToIndex = docsToIndex
        var indexTimeInMillis = 0L
        try {
            if (updatableDocsToIndex.isNotEmpty()) {
                val targetIndex = updatableDocsToIndex.first().index()
                logger.debug("Attempting to index ${updatableDocsToIndex.size} documents to $targetIndex")
                createTargetIndex(targetIndex)
                backoffPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
                    val bulkRequest = BulkRequest().add(updatableDocsToIndex)
                    val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
                    indexTimeInMillis += bulkResponse.took.millis

                    val failed = (bulkResponse.items ?: arrayOf()).filter { item -> item.isFailed }

                    updatableDocsToIndex = failed.map { itemResponse ->
                        updatableDocsToIndex[itemResponse.itemId] as IndexRequest
                    }
                    if (updatableDocsToIndex.isNotEmpty()) {
                        val retryCause = failed.first().failure.cause
                        throw ExceptionsHelper.convertToOpenSearchException(retryCause)
                    }
                }
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
