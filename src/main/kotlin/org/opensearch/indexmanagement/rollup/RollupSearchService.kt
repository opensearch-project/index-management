/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.search.SearchPhaseExecutionException
import org.opensearch.action.search.SearchResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.breaker.CircuitBreakingException
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_SEARCH_BACKOFF_COUNT
import org.opensearch.indexmanagement.rollup.settings.RollupSettings.Companion.ROLLUP_SEARCH_BACKOFF_MILLIS
import org.opensearch.indexmanagement.rollup.util.getRollupSearchRequest
import org.opensearch.search.aggregations.MultiBucketConsumerService
import org.opensearch.transport.RemoteTransportException
import java.time.Instant
import kotlin.math.max
import kotlin.math.pow

// TODO: A cluster level setting to control how many rollups can run at once? Or should we be skipping when cpu/memory/jvm is high?
// Deals with knowing when and how to search the source index
// Knowing when means dealing with time windows and whether or not enough time has passed
// Knowing how means converting the rollup configuration into a composite aggregation
class RollupSearchService(
    settings: Settings,
    clusterService: ClusterService,
    val client: Client
) {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile private var retrySearchPolicy =
        BackoffPolicy.constantBackoff(ROLLUP_SEARCH_BACKOFF_MILLIS.get(settings), ROLLUP_SEARCH_BACKOFF_COUNT.get(settings))

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ROLLUP_SEARCH_BACKOFF_MILLIS, ROLLUP_SEARCH_BACKOFF_COUNT) { millis, count ->
            retrySearchPolicy = BackoffPolicy.constantBackoff(millis, count)
        }
    }

    // TODO: Failed shouldn't process? How to recover from failed -> how does a user retry a failed rollup
    @Suppress("ReturnCount")
    fun shouldProcessRollup(rollup: Rollup, metadata: RollupMetadata?): Boolean {
        if (!rollup.enabled) return false
        // For both continuous and non-continuous rollups if there is an afterKey it means we are still
        // processing data from the current window and should continue to process, the only way we ended up here with an
        // afterKey is if we were still processing data is if the job somehow stopped and was rescheduled (i.e. node crashed etc.)

        // Assuming if this has been called with null metadata, that metadata needs to be initialized
        // TODO: This is mostly used for the check in runJob(), maybe move this out and make that call "metadata == null || shouldProcessRollup"
        //  so that shouldProcessRollup() doesn't let this through in the while loop when rolling up
        if (metadata == null) return true

        if (metadata.status == RollupMetadata.Status.RETRY) return true

        // Being in a STOPPED/FAILED status will take priority over an afterKey being available, user will need to retry
        if (listOf(RollupMetadata.Status.STOPPED, RollupMetadata.Status.FAILED).contains(metadata.status)) {
            return false
        }

        if (metadata.afterKey != null) return true

        if (!rollup.continuous) {
            // If metadata was set to failed and restarted using _start, metadata will be updated to RETRY.
            // After init, metadata will be updated to STARTED, if the failure was before afterKey was ever set,
            // we can end up here in STARTED.
            if (listOf(RollupMetadata.Status.INIT, RollupMetadata.Status.STARTED).contains(metadata.status)) return true
            // If a non-continuous rollup job does not have an afterKey and is not in INIT or STARTED then
            // it can only be FINISHED here since STOPPED and FAILED have already been checked
            logger.debug("Non-continuous job [${rollup.id}] is not processing next window [$metadata]")
            return false
        } else {
            return hasNextFullWindow(rollup, metadata) // TODO: Behavior when next full window but 0 docs/afterkey is null
        }
    }

    private fun hasNextFullWindow(rollup: Rollup, metadata: RollupMetadata): Boolean {
        return Instant.now().isAfter(metadata.continuous!!.nextWindowEndTime.plusMillis(rollup.delay ?: 0)) // TODO: !!
    }

    @Suppress("ComplexMethod")
    suspend fun executeCompositeSearch(job: Rollup, metadata: RollupMetadata): RollupSearchResult {
        return try {
            var retryCount = 0
            RollupSearchResult.Success(
                retrySearchPolicy.retry(logger) {
                    val decay = 2f.pow(retryCount++)
                    client.suspendUntil { listener: ActionListener<SearchResponse> ->
                        val pageSize = max(1, job.pageSize.div(decay.toInt()))
                        if (decay > 1) logger.warn(
                            "Composite search failed for rollup, retrying [#${retryCount - 1}] -" +
                                " reducing page size of composite aggregation from ${job.pageSize} to $pageSize"
                        )
                        search(job.copy(pageSize = pageSize).getRollupSearchRequest(metadata), listener)
                    }
                }
            )
        } catch (e: SearchPhaseExecutionException) {
            logger.error(e.message, e.cause)
            if (e.shardFailures().isEmpty()) {
                RollupSearchResult.Failure(cause = ExceptionsHelper.unwrapCause(e) as Exception)
            } else {
                val shardFailure = e.shardFailures().reduce { s1, s2 -> if (s1.status().status > s2.status().status) s1 else s2 }
                RollupSearchResult.Failure(cause = ExceptionsHelper.unwrapCause(shardFailure.cause) as Exception)
            }
        } catch (e: RemoteTransportException) {
            logger.error(e.message, e.cause)
            RollupSearchResult.Failure(cause = ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: CircuitBreakingException) {
            logger.error(e.message, e.cause)
            RollupSearchResult.Failure(cause = e)
        } catch (e: MultiBucketConsumerService.TooManyBucketsException) {
            logger.error(e.message, e.cause)
            RollupSearchResult.Failure(cause = e)
        } catch (e: OpenSearchSecurityException) {
            logger.error(e.message, e.cause)
            RollupSearchResult.Failure("Cannot search data in source index/s - missing required index permissions: ${e.localizedMessage}", e)
        } catch (e: Exception) {
            logger.error(e.message, e.cause)
            RollupSearchResult.Failure(cause = e)
        }
    }
}

sealed class RollupSearchResult {
    data class Success(val searchResponse: SearchResponse) : RollupSearchResult()
    data class Failure(val message: String = "An error occurred while searching the rollup source index", val cause: Exception) : RollupSearchResult()
}
