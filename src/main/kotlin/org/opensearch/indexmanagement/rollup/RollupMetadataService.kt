/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.common.Rounding
import org.opensearch.common.time.DateFormatter
import org.opensearch.common.time.DateFormatters
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.MatchAllQueryBuilder
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.model.ContinuousMetadata
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.RollupStats
import org.opensearch.indexmanagement.rollup.util.DATE_FIELD_STRICT_DATE_OPTIONAL_TIME_FORMAT
import org.opensearch.indexmanagement.rollup.util.isRollupIndex
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.search.aggregations.bucket.composite.InternalComposite
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.Client
import java.time.Instant

// TODO: Wrap client calls in retry for transient failures
// Service that handles CRUD operations for rollup metadata
@Suppress("TooManyFunctions")
class RollupMetadataService(
    val client: Client,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: org.opensearch.cluster.service.ClusterService,
) {
    private val logger = LogManager.getLogger(javaClass)

    // If the job does not have a metadataID then we need to initialize the first metadata
    // document for this job otherwise we should get the existing metadata document
    @Suppress("ReturnCount", "CyclomaticComplexMethod", "NestedBlockDepth")
    suspend fun init(rollup: Rollup): MetadataResult {
        if (rollup.metadataID != null) {
            val existingMetadata =
                when (val getMetadataResult = getExistingMetadata(rollup)) {
                    is MetadataResult.Success -> getMetadataResult.metadata
                    is MetadataResult.NoMetadata -> null
                    is MetadataResult.Failure -> return getMetadataResult
                }

            if (existingMetadata != null) {
                if (existingMetadata.status == RollupMetadata.Status.RETRY) {
                    val recoveredMetadata =
                        when (val recoverMetadataResult = recoverRetryMetadata(rollup, existingMetadata)) {
                            is MetadataResult.Success -> recoverMetadataResult.metadata

                            // NoMetadata here means that there were no documents when initializing start time
                            // for a continuous rollup so we will propagate the response to no-op in the runner
                            is MetadataResult.NoMetadata -> return recoverMetadataResult

                            // In case of failure, return early with the result
                            is MetadataResult.Failure -> return recoverMetadataResult
                        }

                    // Update to the recovered metadata if recovery was successful
                    return submitMetadataUpdate(recoveredMetadata, true)
                } else {
                    // If metadata exists and was not in RETRY status, return the existing metadata
                    return MetadataResult.Success(existingMetadata)
                }
            } else {
                // The existing metadata was not found, create a new metadata in FAILED status
                return submitMetadataUpdate(
                    RollupMetadata(
                        rollupID = rollup.id, lastUpdatedTime = Instant.now(),
                        status = RollupMetadata.Status.FAILED, failureReason = "Not able to get the rollup metadata [${rollup.metadataID}]",
                        stats = RollupStats(0, 0, 0, 0, 0),
                    ),
                    false,
                )
            }
        }

        val createdMetadataResult = if (rollup.continuous) createContinuousMetadata(rollup) else createNonContinuousMetadata(rollup)
        return when (createdMetadataResult) {
            is MetadataResult.Success -> submitMetadataUpdate(createdMetadataResult.metadata, false)

            // Hitting this case means that there were no documents when initializing start time for a continuous rollup
            is MetadataResult.NoMetadata -> createdMetadataResult

            is MetadataResult.Failure -> createdMetadataResult
        }
    }

    @Suppress("ReturnCount")
    private suspend fun recoverRetryMetadata(rollup: Rollup, metadata: RollupMetadata): MetadataResult {
        var continuousMetadata = metadata.continuous
        if (rollup.continuous && metadata.continuous == null) {
            val nextWindowStartTime =
                when (val initStartTimeResult = getInitialStartTime(rollup)) {
                    is StartingTimeResult.Success -> initStartTimeResult.startingTime

                    is StartingTimeResult.NoDocumentsFound -> return MetadataResult.NoMetadata

                    is StartingTimeResult.Failure ->
                        return MetadataResult.Failure("Failed to initialize start time for retried rollup job [${rollup.id}]", initStartTimeResult.e)
                }
            val nextWindowEndTime = getShiftedTime(nextWindowStartTime, rollup)
            continuousMetadata = ContinuousMetadata(nextWindowStartTime, nextWindowEndTime)
        }

        return MetadataResult.Success(
            metadata.copy(
                continuous = continuousMetadata,
                status = RollupMetadata.Status.STARTED,
            ),
        )
    }

    // This returns the first instantiation of a RollupMetadata for a non-continuous rollup
    private fun createNonContinuousMetadata(rollup: Rollup): MetadataResult =
        MetadataResult.Success(
            RollupMetadata(
                rollupID = rollup.id, lastUpdatedTime = Instant.now(), status = RollupMetadata.Status.INIT,
                stats = RollupStats(0, 0, 0, 0, 0),
            ),
        )

    // This updates the metadata for a non-continuous rollup after an execution of the composite search and ingestion of rollup data
    private fun getUpdatedNonContinuousMetadata(
        metadata: RollupMetadata,
        internalComposite: InternalComposite,
    ): RollupMetadata {
        val afterKey = internalComposite.afterKey()
        return metadata.copy(
            afterKey = afterKey,
            lastUpdatedTime = Instant.now(),
            status = if (afterKey == null) RollupMetadata.Status.FINISHED else RollupMetadata.Status.STARTED,
        )
    }

    // This returns the first instantiation of a RollupMetadata for a continuous rollup
    @Suppress("ReturnCount")
    private suspend fun createContinuousMetadata(rollup: Rollup): MetadataResult {
        val nextWindowStartTime =
            when (val initStartTimeResult = getInitialStartTime(rollup)) {
                is StartingTimeResult.Success -> initStartTimeResult.startingTime

                is StartingTimeResult.NoDocumentsFound -> return MetadataResult.NoMetadata

                is StartingTimeResult.Failure ->
                    return MetadataResult.Failure("Failed to initialize start time for rollup [${rollup.id}]", initStartTimeResult.e)
            }
        // The first end time is just the next window start time
        val nextWindowEndTime = getShiftedTime(nextWindowStartTime, rollup)
        return MetadataResult.Success(
            RollupMetadata(
                rollupID = rollup.id,
                afterKey = null,
                lastUpdatedTime = Instant.now(),
                continuous = ContinuousMetadata(nextWindowStartTime, nextWindowEndTime),
                status = RollupMetadata.Status.INIT,
                failureReason = null,
                stats = RollupStats(0, 0, 0, 0, 0),
            ),
        )
    }

    // TODO: Let User specify their own filter query that is applied to the composite agg search
    @Suppress("ReturnCount")
    @Throws(Exception::class)
    private suspend fun getInitialStartTime(rollup: Rollup): StartingTimeResult {
        try {
            // Check if source is a rollup index and use appropriate method
            val isSourceRollupIndex = isRollupIndex(rollup.sourceIndex, clusterService.state())
            if (isSourceRollupIndex) {
                // Use min aggregation for rollup indices (RollupInterceptor blocks size > 0)
                return getEarliestTimestampFromRollupIndex(rollup)
            }
            // Rollup requires the first dimension to be the date histogram
            val dateHistogram = rollup.dimensions.first() as DateHistogram
            val searchSourceBuilder =
                SearchSourceBuilder()
                    .size(1)
                    .query(MatchAllQueryBuilder())
                    .sort(dateHistogram.sourceField, SortOrder.ASC) // TODO: figure out where nulls are sorted
                    .trackTotalHits(false)
                    .fetchSource(false)
                    .docValueField(dateHistogram.sourceField, DATE_FIELD_STRICT_DATE_OPTIONAL_TIME_FORMAT)
            val searchRequest =
                SearchRequest(rollup.sourceIndex)
                    .source(searchSourceBuilder)
                    .allowPartialSearchResults(false)
            val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }

            if (response.hits.hits.isEmpty()) {
                // Empty doc hits will result in a no-op from the runner
                return StartingTimeResult.NoDocumentsFound
            }

            // Get the doc value field of the dateHistogram.sourceField for the first search hit converted to epoch millis
            // If the doc value is null or empty it will be treated the same as empty doc hits
            val firstHitTimestampAsString: String =
                response.hits.hits.first().field(dateHistogram.sourceField).getValue<String>()
                    ?: return StartingTimeResult.NoDocumentsFound
            // Parse date and extract epochMillis
            val formatter = DateFormatter.forPattern(DATE_FIELD_STRICT_DATE_OPTIONAL_TIME_FORMAT)
            val epochMillis = DateFormatters.from(formatter.parse(firstHitTimestampAsString), formatter.locale()).toInstant().toEpochMilli()
            return StartingTimeResult.Success(getRoundedTime(epochMillis, dateHistogram))
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.error("Error when getting initial start time for rollup [{}]: {}", rollup.id, unwrappedException)
            return StartingTimeResult.Failure(unwrappedException)
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            logger.error("Error when getting initial start time for rollup [{}]: {}", rollup.id, e)
            return StartingTimeResult.Failure(e)
        }
    }

    /**
     * Get the earliest timestamp from a rollup index by finding the minimum value of the date histogram field.
     * This is used to determine the starting point for continuous rollups on rollup indices.
     * Uses sort instead of aggregation to avoid rollup interceptor validation.
     */
    @Suppress("ReturnCount")
    @Throws(Exception::class)
    private suspend fun getEarliestTimestampFromRollupIndex(rollup: Rollup): StartingTimeResult {
        try {
            val dateHistogram = rollup.dimensions.first() as DateHistogram
            val dateField = dateHistogram.sourceField

            logger.info("Idhr se ja rha hu mai tumhe kya 2")

            val searchRequest = SearchRequest(rollup.sourceIndex)
                .source(
                    SearchSourceBuilder()
                        .size(1)
                        .query(MatchAllQueryBuilder())
                        .sort("$dateField.date_histogram", SortOrder.ASC)
                        .trackTotalHits(false)
                        .fetchSource(false)
                        .docValueField("$dateField.date_histogram", DATE_FIELD_STRICT_DATE_OPTIONAL_TIME_FORMAT),
                )
                .allowPartialSearchResults(false)

            // Set BYPASS_SIZE_CHECK to allow size=1 when querying rollup index to get earliest timestamp
            // This is needed for continuous rollup initialization on rollup indices (multi-tier rollup)
            org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.setBypass(
                org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.BYPASS_SIZE_CHECK,
            )
            try {
                val response: SearchResponse = client.suspendUntil { search(searchRequest, it) }

                if (response.hits.hits.isEmpty()) {
                    return StartingTimeResult.NoDocumentsFound
                }

                logger.info("Idhr se ja rha hu mai tumhe kya")

                // In rollup indices, date histogram fields are named as "field.date_histogram"
                val rollupDateField = "$dateField.date_histogram"
                val firstHitTimestampAsString: String =
                    response.hits.hits.first().field(rollupDateField).getValue<String>()
                        ?: return StartingTimeResult.NoDocumentsFound

                val formatter = DateFormatter.forPattern(DATE_FIELD_STRICT_DATE_OPTIONAL_TIME_FORMAT)
                val epochMillis = DateFormatters.from(formatter.parse(firstHitTimestampAsString), formatter.locale()).toInstant().toEpochMilli()
                return StartingTimeResult.Success(getRoundedTime(epochMillis, dateHistogram))
            } finally {
                org.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor.clearBypass()
            }
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.error("Error when getting earliest timestamp from rollup index for rollup [{}]: {}", rollup.id, unwrappedException)
            return StartingTimeResult.Failure(unwrappedException)
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            logger.error("Error when getting earliest timestamp from rollup index for rollup [{}]: {}", rollup.id, e)
            return StartingTimeResult.Failure(e)
        }
    }

    /**
     * Return time rounded down to the nearest unit of time the interval is based on.
     * This should map to the equivalent bucket a document with the given timestamp would fall into for the date histogram.
     */
    private fun getRoundedTime(timestamp: Long, dateHistogram: DateHistogram): Instant {
        val roundingStrategy = getRoundingStrategy(dateHistogram)
        val roundedMillis =
            roundingStrategy
                .prepare(timestamp, timestamp)
                .round(timestamp)
        return Instant.ofEpochMilli(roundedMillis)
    }

    /** Takes an existing start or end time and returns the value for the next window based on the rollup interval */
    private fun getShiftedTime(time: Instant, rollup: Rollup): Instant {
        val dateHistogram = rollup.dimensions.first() as DateHistogram
        val roundingStrategy = getRoundingStrategy(dateHistogram)

        val timeInMillis = time.toEpochMilli()
        val nextRoundedMillis =
            roundingStrategy
                .prepare(timeInMillis, timeInMillis)
                .nextRoundingValue(timeInMillis)
        return Instant.ofEpochMilli(nextRoundedMillis)
    }

    // TODO: Could make this an extension function of DateHistogram and add to some utility file

    /**
     * Get the rounding strategy for the given time interval in the DateHistogram.
     * This is used to calculate time windows by rounding the given time based on the interval.
     */
    private fun getRoundingStrategy(dateHistogram: DateHistogram): Rounding {
        val intervalString = (dateHistogram.calendarInterval ?: dateHistogram.fixedInterval) as String
        // TODO: Make sure the interval string is validated before getting here so we don't get errors
        return if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(intervalString)) {
            // Calendar intervals should be handled here
            val intervalUnit: Rounding.DateTimeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS[intervalString]!!
            Rounding.builder(intervalUnit)
                .timeZone(dateHistogram.timezone)
                .build()
        } else {
            // Fixed intervals are handled here
            val timeValue = TimeValue.parseTimeValue(intervalString, "RollupMetadataService#getRoundingStrategy")
            Rounding.builder(timeValue)
                .timeZone(dateHistogram.timezone)
                .build()
        }
    }

    // This updates the metadata for a continuous rollup after an execution of the composite search and ingestion of rollup data
    private fun getUpdatedContinuousMetadata(
        rollup: Rollup,
        metadata: RollupMetadata,
        internalComposite: InternalComposite,
    ): RollupMetadata {
        val afterKey = internalComposite.afterKey()
        // TODO: get rid of !!
        val nextStart =
            if (afterKey == null) {
                getShiftedTime(metadata.continuous!!.nextWindowStartTime, rollup)
            } else {
                metadata.continuous!!.nextWindowStartTime
            }
        val nextEnd =
            if (afterKey == null) {
                getShiftedTime(metadata.continuous.nextWindowEndTime, rollup)
            } else {
                metadata.continuous.nextWindowEndTime
            }
        return metadata.copy(
            afterKey = internalComposite.afterKey(),
            lastUpdatedTime = Instant.now(),
            continuous = ContinuousMetadata(nextStart, nextEnd),
            status = RollupMetadata.Status.STARTED,
        )
    }

    @Suppress("BlockingMethodInNonBlockingContext", "ReturnCount")
    suspend fun getExistingMetadata(rollup: Rollup): MetadataResult {
        val errorMessage = "Error when getting rollup metadata [${rollup.metadataID}]"
        try {
            var rollupMetadata: RollupMetadata? = null
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, rollup.metadataID).routing(rollup.id)
            val response: GetResponse = client.suspendUntil { get(getRequest, it) }

            if (!response.isExists) return MetadataResult.NoMetadata

            val metadataSource = response.sourceAsBytesRef
            metadataSource?.let {
                withContext(Dispatchers.IO) {
                    val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, metadataSource, XContentType.JSON)
                    rollupMetadata = xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, RollupMetadata.Companion::parse)
                }
            }

            return if (rollupMetadata != null) {
                MetadataResult.Success(rollupMetadata!!)
            } else {
                MetadataResult.NoMetadata
            }
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.error("$errorMessage: $unwrappedException")
            return MetadataResult.Failure(errorMessage, unwrappedException)
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            logger.error("$errorMessage: $e")
            return MetadataResult.Failure(errorMessage, e)
        }
    }

    suspend fun updateMetadata(rollup: Rollup, metadata: RollupMetadata, internalComposite: InternalComposite): RollupMetadata {
        val updatedMetadata =
            if (rollup.continuous) {
                getUpdatedContinuousMetadata(rollup, metadata, internalComposite)
            } else {
                getUpdatedNonContinuousMetadata(metadata, internalComposite)
            }

        return updateMetadata(updatedMetadata)
    }

    suspend fun updateMetadata(metadata: RollupMetadata): RollupMetadata =
        when (val metadataUpdateResult = submitMetadataUpdate(metadata, metadata.id != NO_ID)) {
            is MetadataResult.Success -> metadataUpdateResult.metadata

            is MetadataResult.Failure ->
                throw RollupMetadataException("Failed to update rollup metadata [${metadata.id}]", metadataUpdateResult.cause)

            // NoMetadata is not expected from submitMetadataUpdate here
            is MetadataResult.NoMetadata -> throw RollupMetadataException("Unexpected state when updating rollup metadata [${metadata.id}]", null)
        }

    /**
     * Sets a failure metadata for the rollup job with the given reason.
     * Can provide an existing metadata to update, if none are provided a new metadata is created
     * to replace the current one for the job.
     */
    suspend fun setFailedMetadata(job: Rollup, reason: String, existingMetadata: RollupMetadata? = null): MetadataResult {
        val updatedMetadata: RollupMetadata?
        if (existingMetadata == null) {
            // Create new metadata
            updatedMetadata =
                RollupMetadata(
                    rollupID = job.id,
                    status = RollupMetadata.Status.FAILED,
                    failureReason = reason,
                    lastUpdatedTime = Instant.now(),
                    stats = RollupStats(0, 0, 0, 0, 0),
                )
        } else {
            // Update the given existing metadata
            updatedMetadata =
                existingMetadata.copy(
                    status = RollupMetadata.Status.FAILED,
                    failureReason = reason,
                    lastUpdatedTime = Instant.now(),
                )
        }

        return submitMetadataUpdate(updatedMetadata, updatedMetadata.id != NO_ID)
    }

    @Suppress("CyclomaticComplexMethod", "ReturnCount")
    private suspend fun submitMetadataUpdate(metadata: RollupMetadata, updating: Boolean): MetadataResult {
        val errorMessage = "An error occurred when ${if (updating) "updating" else "creating"} rollup metadata"
        try {
            @Suppress("BlockingMethodInNonBlockingContext")
            val builder =
                XContentFactory.jsonBuilder().startObject()
                    .field(RollupMetadata.ROLLUP_METADATA_TYPE, metadata)
                    .endObject()
            val indexRequest = IndexRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).source(builder).routing(metadata.rollupID)
            if (updating) {
                indexRequest.id(metadata.id).setIfSeqNo(metadata.seqNo).setIfPrimaryTerm(metadata.primaryTerm)
            } else {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            }

            val response: IndexResponse = client.suspendUntil { index(indexRequest, it) }
            var status: RollupMetadata.Status = metadata.status
            var failureReason: String? = metadata.failureReason
            when (response.result) {
                DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED -> {
                    // noop
                }

                DocWriteResponse.Result.DELETED, DocWriteResponse.Result.NOOP, DocWriteResponse.Result.NOT_FOUND, null -> {
                    status = RollupMetadata.Status.FAILED
                    failureReason = "The create metadata call failed with a ${response.result?.lowercase} result"
                }
            }
            logger.debug("Metadata update successful {}", metadata)
            // TODO: Is seqno/prim and id returned for all?
            return MetadataResult.Success(
                metadata.copy(
                    id = response.id,
                    seqNo = response.seqNo,
                    primaryTerm = response.primaryTerm,
                    status = status,
                    failureReason = failureReason,
                ),
            )
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.error("Metadata update failed ${metadata.rollupID}", unwrappedException)
            return MetadataResult.Failure(errorMessage, unwrappedException)
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            logger.error("Metadata update failed ${metadata.rollupID}", e)
            return MetadataResult.Failure(errorMessage, e)
        }
    }
}

sealed class MetadataResult {
    // A successful MetadataResult just means a metadata was returned,
    // it can still have a FAILED status
    data class Success(val metadata: RollupMetadata) : MetadataResult()

    data class Failure(val message: String = "An error occurred for rollup metadata", val cause: Exception) : MetadataResult()

    object NoMetadata : MetadataResult()
}

sealed class StartingTimeResult {
    data class Success(val startingTime: Instant) : StartingTimeResult()

    data class Failure(val e: Exception) : StartingTimeResult()

    object NoDocumentsFound : StartingTimeResult()
}

class RollupMetadataException(message: String, cause: Throwable?) : Exception(message, cause)
