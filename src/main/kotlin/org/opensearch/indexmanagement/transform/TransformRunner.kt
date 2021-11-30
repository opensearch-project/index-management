/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.opensearchapi.IndexManagementSecurityContext
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.opensearchapi.withClosableContext
import org.opensearch.indexmanagement.transform.action.index.IndexTransformAction
import org.opensearch.indexmanagement.transform.action.index.IndexTransformRequest
import org.opensearch.indexmanagement.transform.action.index.IndexTransformResponse
import org.opensearch.indexmanagement.transform.model.ShardNewDocuments
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.model.TransformSearchResult
import org.opensearch.indexmanagement.transform.model.TransformStats
import org.opensearch.indexmanagement.transform.settings.TransformSettings
import org.opensearch.indexmanagement.util.acquireLockForScheduledJob
import org.opensearch.indexmanagement.util.releaseLockForScheduledJob
import org.opensearch.indexmanagement.util.renewLockForScheduledJob
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.ScheduledJobRunner
import org.opensearch.monitor.jvm.JvmService
import org.opensearch.threadpool.ThreadPool
import java.time.Instant

@Suppress("LongParameterList")
object TransformRunner :
    ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("TransformRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var settings: Settings
    private lateinit var transformMetadataService: TransformMetadataService
    private lateinit var transformSearchService: TransformSearchService
    private lateinit var transformIndexer: TransformIndexer
    private lateinit var transformValidator: TransformValidator
    private lateinit var threadPool: ThreadPool

    fun initialize(
        client: Client,
        clusterService: ClusterService,
        xContentRegistry: NamedXContentRegistry,
        settings: Settings,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        jvmService: JvmService,
        threadPool: ThreadPool
    ): TransformRunner {
        this.clusterService = clusterService
        this.client = client
        this.xContentRegistry = xContentRegistry
        this.settings = settings
        this.transformSearchService = TransformSearchService(settings, clusterService, client)
        this.transformMetadataService = TransformMetadataService(client, xContentRegistry)
        this.transformIndexer = TransformIndexer(settings, clusterService, client)
        this.transformValidator = TransformValidator(indexNameExpressionResolver, clusterService, client, settings, jvmService)
        this.threadPool = threadPool
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is Transform) {
            throw IllegalArgumentException("Received invalid job type [${job.javaClass.simpleName}] with id [${context.jobId}]")
        }

        launch {
            try {
                if (job.enabled) {
                    val metadata = transformMetadataService.getMetadata(job)
                    var transform = job
                    if (job.metadataId == null) {
                        transform = updateTransform(job.copy(metadataId = metadata.id))
                    }
                    executeJob(transform, metadata, context)
                }
            } catch (e: Exception) {
                logger.error("Failed to run job [${job.id}] because ${e.localizedMessage}", e)
                return@launch
            }
        }
    }

    // TODO: Add circuit breaker checks - [cluster healthy, utilization within limit]
    @Suppress("NestedBlockDepth", "ComplexMethod")
    private suspend fun executeJob(transform: Transform, metadata: TransformMetadata, context: JobExecutionContext) {
        var currentMetadata = metadata
        val backoffPolicy = BackoffPolicy.exponentialBackoff(
            TimeValue.timeValueMillis(TransformSettings.DEFAULT_RENEW_LOCK_RETRY_DELAY),
            TransformSettings.DEFAULT_RENEW_LOCK_RETRY_COUNT
        )
        var lock = acquireLockForScheduledJob(transform, context, backoffPolicy)

        var currentShard: ShardNewDocuments? = null
        var shardsToSearch: Iterator<ShardNewDocuments>? = null
        val oldShardIDToMaxSeqNo = metadata.shardIDToGlobalCheckpoint
        try {
            do {
                when {
                    lock == null -> {
                        logger.warn("Cannot acquire lock for transform job ${transform.id}")
                        // If we fail to get the lock we won't fail the job, instead we return early
                        return
                    }
                    listOf(TransformMetadata.Status.STOPPED, TransformMetadata.Status.FINISHED).contains(metadata.status) -> {
                        logger.warn("Transform job ${transform.id} is in ${metadata.status} status. Skipping execution")
                        return
                    }
                    else -> {
                        if (transform.continuous && shardsToSearch == null) {
                            currentMetadata = withTransformSecurityContext(transform) {
                                currentMetadata.copy(
                                    shardIDToGlobalCheckpoint = transformSearchService.getShardsGlobalCheckpoint(transform.sourceIndex),
                                    continuousStats = currentMetadata.continuousStats?.copy(lastTimestamp = Instant.now())
                                )
                            }
                            shardsToSearch = getShardsToSearch(oldShardIDToMaxSeqNo, currentMetadata.shardIDToGlobalCheckpoint!!).iterator()
                            if (shardsToSearch.hasNext()) currentShard = shardsToSearch.next()
                        }
                        currentMetadata = executeJobIteration(transform, currentMetadata, currentShard)
                        if (currentMetadata.afterKey == null && transform.continuous) {
                            currentShard = if (shardsToSearch!!.hasNext()) {
                                shardsToSearch.next()
                            } else break
                        }
                        // we attempt to renew lock for every loop of transform
                        val renewedLock = renewLockForScheduledJob(context, lock, backoffPolicy)
                        if (renewedLock == null) {
                            releaseLockForScheduledJob(context, lock)
                        }
                        lock = renewedLock
                    }
                }
            } while (currentMetadata.afterKey != null || (transform.continuous && currentShard != null))
        } catch (e: Exception) {
            logger.error("Failed to execute the transform job [${transform.id}] because of exception [${e.localizedMessage}]", e)
            currentMetadata = currentMetadata.copy(
                lastUpdatedAt = Instant.now(),
                status = TransformMetadata.Status.FAILED,
                failureReason = e.localizedMessage
            )
        } finally {
            lock?.let {
                transformMetadataService.writeMetadata(currentMetadata, true)
                if (!transform.continuous || currentMetadata.status == TransformMetadata.Status.FAILED) {
                    logger.info("Disabling the transform job ${transform.id}")
                    updateTransform(transform.copy(enabled = false, enabledAt = null))
                }
                releaseLockForScheduledJob(context, it)
            }
        }
    }

    // Processes through the old and new maps of sequence numbers to generate a list of objects with the shardId and range of sequence numbers to search
    private fun getShardsToSearch(oldShardIDToMaxSeqNo: Map<ShardId, Long>?, newShardIDToMaxSeqNo: Map<ShardId, Long>): List<ShardNewDocuments> {
        val shardsToSearch: MutableList<ShardNewDocuments> = ArrayList()
        newShardIDToMaxSeqNo.forEach { (shardId, currentMaxSeqNo) ->
            if ((oldShardIDToMaxSeqNo == null) || (shardId !in oldShardIDToMaxSeqNo.keys) || (currentMaxSeqNo > oldShardIDToMaxSeqNo[shardId]!!)) {
                shardsToSearch.add(ShardNewDocuments(shardId, oldShardIDToMaxSeqNo?.get(shardId), currentMaxSeqNo))
            }
        }
        return shardsToSearch
    }

    private suspend fun validateTransform(transform: Transform, transformMetadata: TransformMetadata): TransformMetadata {
        val validationResult = withTransformSecurityContext(transform) {
            transformValidator.validate(transform)
        }
        return if (!validationResult.isValid) {
            val failureMessage = "Failed validation - ${validationResult.issues}"
            val failureMetadata = transformMetadata.copy(status = TransformMetadata.Status.FAILED, failureReason = failureMessage)
            transformMetadataService.writeMetadata(failureMetadata, true)
        } else transformMetadata
    }

    private suspend fun executeJobIteration(transform: Transform, metadata: TransformMetadata, currentShard: ShardNewDocuments?): TransformMetadata {
        val validatedMetadata = validateTransform(transform, metadata)
        if (validatedMetadata.status == TransformMetadata.Status.FAILED) {
            return validatedMetadata
        }
        // If currentShard is null here, then there were no shards or new data to transform
        return if (transform.continuous && currentShard != null) {
            executeContinuousJobIteration(transform, validatedMetadata, currentShard)
        } else if (!transform.continuous) {
            executeNonContinuousJobIteration(transform, validatedMetadata)
        } else metadata.copy(afterKey = null) // afterKey should already be null, but this would prevent an infinite loop if it isn't null
    }

    private suspend fun executeContinuousJobIteration(transform: Transform, metadata: TransformMetadata, currentShard: ShardNewDocuments): TransformMetadata {
        val bucketSearchResult = withTransformSecurityContext(transform) {
            transformSearchService.getModifiedBuckets(transform, metadata.afterKey, currentShard)
        }
        val transformSearchResult = transformModifiedBuckets(transform, bucketSearchResult.modifiedBuckets)
        val indexTimeInMillis = withTransformSecurityContext(transform) {
            transformIndexer.index(transformSearchResult.docsToIndex)
        }
        val afterKey = bucketSearchResult.afterKey
        val stats = transformSearchResult.stats
        val updatedStats = stats.copy(
            indexTimeInMillis = stats.indexTimeInMillis + indexTimeInMillis,
            documentsIndexed = transformSearchResult.docsToIndex.size.toLong(),
            searchTimeInMillis = stats.searchTimeInMillis + bucketSearchResult.searchTimeInMillis
        )
        val updatedMetadata = metadata.mergeStats(updatedStats).copy(
            afterKey = afterKey,
            lastUpdatedAt = Instant.now(),
            status = TransformMetadata.Status.STARTED
        )
        return transformMetadataService.writeMetadata(updatedMetadata, true)
    }

    /**
     * For a continuous transform, we paginate over the set of modified buckets, however, with a histogram grouping and a decimal interval,
     * the range query will not precisely specify the modified buckets. As a result, we increase the range for the query and then filter out
     * the unintended buckets. As we may end up with more buckets than we had from the set of modified buckets, we need to do a second pagination
     * here, though it will not often be used.
     */
    private suspend fun transformModifiedBuckets(transform: Transform, modifiedBuckets: MutableSet<Map<String, Any>>): TransformSearchResult {
        // Maintain pagesProcessed as 1 in this step, as pageSize should only track buckets for a continuous transform
        var transformSearchResult = TransformSearchResult(TransformStats(1, 0, 0, 0, 0), ArrayList())
        do {
            val searchResult = withTransformSecurityContext(transform) {
                transformSearchService.executeCompositeSearch(transform, transformSearchResult.afterKey, modifiedBuckets)
            }
            val updatedStats = searchResult.stats.copy(
                documentsProcessed = searchResult.stats.documentsProcessed + transformSearchResult.stats.documentsProcessed,
                searchTimeInMillis = searchResult.stats.searchTimeInMillis + transformSearchResult.stats.searchTimeInMillis
            )
            val updatedDocsToIndex = searchResult.docsToIndex + transformSearchResult.docsToIndex
            transformSearchResult = transformSearchResult.copy(stats = updatedStats, docsToIndex = updatedDocsToIndex, afterKey = searchResult.afterKey)
        } while (transformSearchResult.afterKey != null)
        return transformSearchResult
    }

    private suspend fun executeNonContinuousJobIteration(transform: Transform, metadata: TransformMetadata): TransformMetadata {
        val transformSearchResult = withTransformSecurityContext(transform) {
            transformSearchService.executeCompositeSearch(transform, metadata.afterKey)
        }
        val indexTimeInMillis = withTransformSecurityContext(transform) {
            transformIndexer.index(transformSearchResult.docsToIndex)
        }
        val afterKey = transformSearchResult.afterKey
        val stats = transformSearchResult.stats
        val updatedStats = stats.copy(
            indexTimeInMillis = stats.indexTimeInMillis + indexTimeInMillis, documentsIndexed = transformSearchResult.docsToIndex.size.toLong()
        )
        val updatedMetadata = metadata.mergeStats(updatedStats).copy(
            afterKey = afterKey,
            lastUpdatedAt = Instant.now(),
            status = if (afterKey == null) TransformMetadata.Status.FINISHED else TransformMetadata.Status.STARTED
        )
        return transformMetadataService.writeMetadata(updatedMetadata, true)
    }

    private suspend fun <T> withTransformSecurityContext(transform: Transform, block: suspend CoroutineScope.() -> T): T {
        return withClosableContext(IndexManagementSecurityContext(transform.id, settings, threadPool.threadContext, transform.user), block)
    }

    private suspend fun updateTransform(transform: Transform): Transform {
        val request = IndexTransformRequest(
            transform = transform.copy(updatedAt = Instant.now()),
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        )
        return withClosableContext(
            IndexManagementSecurityContext(transform.id, settings, threadPool.threadContext, null)
        ) {
            val response: IndexTransformResponse = client.suspendUntil {
                execute(IndexTransformAction.INSTANCE, request, it)
            }
            return@withClosableContext transform.copy(
                seqNo = response.seqNo,
                primaryTerm = response.primaryTerm
            )
        }
    }
}
