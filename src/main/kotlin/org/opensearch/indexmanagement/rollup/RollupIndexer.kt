/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkItemResponse
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupStats
import org.opensearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_INGEST_BACKOFF_COUNT
import org.opensearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_INGEST_BACKOFF_MILLIS
import org.opensearch.indexmanagement.rollup.util.RollupFieldValueExpressionResolver
import org.opensearch.indexmanagement.rollup.util.getInitialDocValues
import org.opensearch.indexmanagement.util.IndexUtils.Companion.ODFE_MAGIC_NULL
import org.opensearch.indexmanagement.util.IndexUtils.Companion.hashToFixedSize
import org.opensearch.rest.RestStatus
import org.opensearch.search.aggregations.bucket.composite.InternalComposite
import org.opensearch.search.aggregations.metrics.InternalAvg
import org.opensearch.search.aggregations.metrics.InternalMax
import org.opensearch.search.aggregations.metrics.InternalMin
import org.opensearch.search.aggregations.metrics.InternalSum
import org.opensearch.search.aggregations.metrics.InternalValueCount
import org.opensearch.transport.RemoteTransportException

@Suppress("ThrowsCount", "ComplexMethod")
class RollupIndexer(
    settings: Settings,
    clusterService: ClusterService,
    private val client: Client
) {
    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var retryIngestPolicy =
        BackoffPolicy.constantBackoff(ROLLUP_INGEST_BACKOFF_MILLIS.get(settings), ROLLUP_INGEST_BACKOFF_COUNT.get(settings))

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ROLLUP_INGEST_BACKOFF_MILLIS, ROLLUP_INGEST_BACKOFF_COUNT) { millis, count ->
            retryIngestPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    @Suppress("ReturnCount")
    suspend fun indexRollups(rollup: Rollup, internalComposite: InternalComposite): RollupIndexResult {
        try {
            var requestsToRetry = convertSearchRespToIndexReqs(rollup, internalComposite)
            var stats = RollupStats(0, 0, requestsToRetry.size.toLong(), 0, 0)

            val retryableFailures = mutableListOf<BulkItemResponse>()
            val nonRetryableFailures = mutableListOf<BulkItemResponse>()
            if (requestsToRetry.isNotEmpty()) {
                retryIngestPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) { backOffTime ->
                    if (backOffTime.seconds >= (Rollup.ROLLUP_LOCK_DURATION_SECONDS / 2)) {
                        throw ExceptionsHelper.convertToOpenSearchException(
                            IllegalStateException(
                                "Cannot retry ingestion with a backoff (${backOffTime.seconds}) " +
                                    "more than half of the rollup lock duration (1800s)"
                            )
                        )
                    }
                    val bulkRequest = BulkRequest().add(requestsToRetry)
                    val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
                    stats = stats.copy(indexTimeInMillis = stats.indexTimeInMillis + bulkResponse.took.millis)

                    (bulkResponse.items ?: arrayOf()).filter { it.isFailed }.forEach { failedResponse ->
                        if (failedResponse.status() == RestStatus.TOO_MANY_REQUESTS) {
                            retryableFailures.add(failedResponse)
                        } else {
                            nonRetryableFailures.add(failedResponse)
                        }
                    }
                    requestsToRetry = retryableFailures.map { retryableFailure -> bulkRequest.requests()[retryableFailure.itemId] as IndexRequest }

                    if (requestsToRetry.isNotEmpty()) {
                        val retryCause = retryableFailures.first().failure.cause
                        throw ExceptionsHelper.convertToOpenSearchException(retryCause)
                    }
                }
            }
            if (nonRetryableFailures.isNotEmpty()) {
                logger.error("Failed to index ${nonRetryableFailures.size} documents")
                throw ExceptionsHelper.convertToOpenSearchException(nonRetryableFailures.first().failure.cause)
            }
            return RollupIndexResult.Success(stats)
        } catch (e: RemoteTransportException) {
            logger.error(e.message, e.cause)
            return RollupIndexResult.Failure(cause = ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            logger.error(e.message, e.cause)
            return RollupIndexResult.Failure(cause = e)
        }
    }

    @Suppress("ComplexMethod")
    private fun convertSearchRespToIndexReqs(rollup: Rollup, internalComposite: InternalComposite): List<DocWriteRequest<*>> {
        val requests = mutableListOf<DocWriteRequest<*>>()
        internalComposite.buckets.forEach { bucket ->
            val docId = rollup.id + "#" + bucket.key.entries.joinToString("#") { it.value?.toString() ?: ODFE_MAGIC_NULL }
            val documentId = hashToFixedSize(docId)

            val mapOfKeyValues = rollup.getInitialDocValues(bucket.docCount)
            val aggResults = mutableMapOf<String, Any?>()
            bucket.key.entries.forEach { aggResults[it.key] = it.value }
            bucket.aggregations.forEach {
                when (it) {
                    is InternalSum -> aggResults[it.name] = it.value
                    is InternalMax -> aggResults[it.name] = it.value
                    is InternalMin -> aggResults[it.name] = it.value
                    is InternalValueCount -> aggResults[it.name] = it.value
                    is InternalAvg -> aggResults[it.name] = it.value
                    else -> error("Found aggregation in composite result that is not supported [${it.type} - ${it.name}]")
                }
            }
            mapOfKeyValues.putAll(aggResults)

            val indexRequest = IndexRequest(RollupFieldValueExpressionResolver.resolve(rollup, rollup.targetIndex))
                .id(documentId).source(mapOfKeyValues, XContentType.JSON)
            requests.add(indexRequest)
        }
        return requests
    }
}

sealed class RollupIndexResult {
    data class Success(val stats: RollupStats) : RollupIndexResult()
    data class Failure(
        val message: String = "An error occurred while indexing to the rollup target index",
        val cause: Exception
    ) : RollupIndexResult()
}
