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
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.bulk.BackoffPolicy
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
import org.opensearch.rest.RestStatus
import org.opensearch.transport.RemoteTransportException

class TransformIndexer(
    settings: Settings,
    clusterService: ClusterService,
    private val esClient: Client
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

    @Suppress("ThrowsCount")
    suspend fun index(docsToIndex: List<DocWriteRequest<*>>): Long {
        var updatableDocsToIndex = docsToIndex
        var indexTimeInMillis = 0L
        try {
            if (updatableDocsToIndex.isNotEmpty()) {
                logger.debug("Attempting to index ${updatableDocsToIndex.size} documents to ${updatableDocsToIndex.first().index()}")
                backoffPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
                    val bulkRequest = BulkRequest().add(updatableDocsToIndex)
                    val bulkResponse: BulkResponse = esClient.suspendUntil { bulk(bulkRequest, it) }
                    indexTimeInMillis += bulkResponse.took.millis

                    val failed = (bulkResponse.items ?: arrayOf()).filter { item -> item.isFailed }

                    updatableDocsToIndex = failed.map { itemResponse ->
                        bulkRequest.requests()[itemResponse.itemId] as IndexRequest
                    }
                    if (updatableDocsToIndex.isNotEmpty()) {
                        val retryCause = failed.first().failure.cause
                        throw ExceptionsHelper.convertToOpenSearchException(retryCause)
                    }
                }
            }
            return indexTimeInMillis
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            throw TransformIndexException("Failed to index the documents", unwrappedException)
        } catch (e: Exception) {
            throw TransformIndexException("Failed to index the documents", e)
        }
    }
}
