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
import org.opensearch.indexmanagement.transform.model.BucketsToTransform
import org.opensearch.indexmanagement.transform.model.ContinuousTransformStats
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.model.initializeShardsToSearch
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

@Suppress("LongParameterList", "TooManyFunctions")
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
    @Suppress("NestedBlockDepth", "ComplexMethod", "LongMethod", "ReturnCount")
    private suspend fun executeJob(transform: Transform, metadata: TransformMetadata, context: JobExecutionContext) {
        var newGlobalCheckpoints: Map<ShardId, Long>? = null
        var newGlobalCheckpointTime: Instant? = null
        var currentMetadata = metadata
        val backoffPolicy = BackoffPolicy.exponentialBackoff(
            TimeValue.timeValueMillis(TransformSettings.DEFAULT_RENEW_LOCK_RETRY_DELAY),
            TransformSettings.DEFAULT_RENEW_LOCK_RETRY_COUNT
        )

        var attemptedToIndex = false
        var bucketsToTransform = BucketsToTransform(HashSet(), metadata)
        var lock = acquireLockForScheduledJob(transform, context, backoffPolicy)
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
                        val validatedMetadata = validateTransform(transform, currentMetadata)
                        if (validatedMetadata.status == TransformMetadata.Status.FAILED) {
                            currentMetadata = validatedMetadata
                            return
                        }
                        if (transform.continuous && (bucketsToTransform.shardsToSearch == null || bucketsToTransform.currentShard != null)) {
                            // If we have not populated the list of shards to search, do so now
                            if (bucketsToTransform.shardsToSearch == null) {
                                // Note the timestamp when we got the shard global checkpoints to the user may know what data is included
                                newGlobalCheckpointTime = Instant.now()
                                newGlobalCheckpoints = transformSearchService.getShardsGlobalCheckpoint(transform.sourceIndex)
                                bucketsToTransform = bucketsToTransform.initializeShardsToSearch(
                                    metadata.shardIDToGlobalCheckpoint,
                                    newGlobalCheckpoints
                                )
                            }
                            bucketsToTransform = getBucketsToTransformIteration(transform, bucketsToTransform)
                            currentMetadata = bucketsToTransform.metadata
                        } else {
                            currentMetadata = executeTransformIteration(transform, currentMetadata, bucketsToTransform.modifiedBuckets)
                            attemptedToIndex = true
                        }
                        // we attempt to renew lock for every loop of transform
                        val renewedLock = renewLockForScheduledJob(context, lock, backoffPolicy)
                        if (renewedLock == null) {
                            releaseLockForScheduledJob(context, lock)
                        }
                        lock = renewedLock
                    }
                }
            } while (bucketsToTransform.currentShard != null || currentMetadata.afterKey != null || !attemptedToIndex)
        } catch (e: Exception) {
            logger.error("Failed to execute the transform job [${transform.id}] because of exception [${e.localizedMessage}]", e)
            currentMetadata = currentMetadata.copy(
                lastUpdatedAt = Instant.now(),
                status = TransformMetadata.Status.FAILED,
                failureReason = e.localizedMessage
            )
        } finally {
            lock?.let {
                // Update the global checkpoints only after execution finishes successfully
                if (transform.continuous && currentMetadata.status != TransformMetadata.Status.FAILED) {
                    currentMetadata = currentMetadata.copy(
                        shardIDToGlobalCheckpoint = newGlobalCheckpoints,
                        continuousStats = ContinuousTransformStats(newGlobalCheckpointTime, null)
                    )
                }
                transformMetadataService.writeMetadata(currentMetadata, true)
                if (!transform.continuous || currentMetadata.status == TransformMetadata.Status.FAILED) {
                    logger.info("Disabling the transform job ${transform.id}")
                    updateTransform(transform.copy(enabled = false, enabledAt = null))
                }
                releaseLockForScheduledJob(context, it)
            }
        }
    }

    private suspend fun getBucketsToTransformIteration(transform: Transform, bucketsToTransform: BucketsToTransform): BucketsToTransform {
        var currentBucketsToTransform = bucketsToTransform
        val currentShard = bucketsToTransform.currentShard

        if (currentShard != null) {
            val shardLevelModifiedBuckets = withTransformSecurityContext(transform) {
                transformSearchService.getShardLevelModifiedBuckets(transform, currentBucketsToTransform.metadata.afterKey, currentShard)
            }
            currentBucketsToTransform.modifiedBuckets.addAll(shardLevelModifiedBuckets.modifiedBuckets)
            val mergedSearchTime = currentBucketsToTransform.metadata.stats.searchTimeInMillis +
                shardLevelModifiedBuckets.searchTimeInMillis
            currentBucketsToTransform = currentBucketsToTransform.copy(
                metadata = currentBucketsToTransform.metadata.copy(
                    stats = currentBucketsToTransform.metadata.stats.copy(
                        pagesProcessed = currentBucketsToTransform.metadata.stats.pagesProcessed + 1,
                        searchTimeInMillis = mergedSearchTime
                    ),
                    afterKey = shardLevelModifiedBuckets.afterKey
                ),
                currentShard = currentShard
            )
        }
        // If finished with this shard, go to the next
        if (currentBucketsToTransform.metadata.afterKey == null) {
            val shardsToSearch = currentBucketsToTransform.shardsToSearch
            currentBucketsToTransform = if (shardsToSearch?.hasNext() == true) {
                currentBucketsToTransform.copy(currentShard = shardsToSearch.next())
            } else {
                currentBucketsToTransform.copy(currentShard = null)
            }
        }
        return currentBucketsToTransform
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

    /**
     * For a continuous transform, we paginate over the set of modified buckets, however, with a histogram grouping and a decimal interval,
     * the range query will not precisely specify the modified buckets. As a result, we increase the range for the query and then filter out
     * the unintended buckets as part of the composite search step.
     */
    private suspend fun executeTransformIteration(
        transform: Transform,
        metadata: TransformMetadata,
        modifiedBuckets: MutableSet<Map<String, Any>>
    ): TransformMetadata {
        val updatedMetadata = if (!transform.continuous || modifiedBuckets.isNotEmpty()) {
            val transformSearchResult = withTransformSecurityContext(transform) {
                transformSearchService.executeCompositeSearch(transform, metadata.afterKey, if (transform.continuous) modifiedBuckets else null)
            }
            val indexTimeInMillis = withTransformSecurityContext(transform) {
                transformIndexer.index(transformSearchResult.docsToIndex)
            }
            val afterKey = transformSearchResult.afterKey
            val stats = transformSearchResult.stats
            val updatedStats = stats.copy(
                pagesProcessed = if (transform.continuous) 0 else stats.pagesProcessed,
                indexTimeInMillis = stats.indexTimeInMillis + indexTimeInMillis,
                documentsIndexed = transformSearchResult.docsToIndex.size.toLong()
            )
            metadata.mergeStats(updatedStats).copy(
                afterKey = afterKey,
                lastUpdatedAt = Instant.now(),
                status = if (afterKey == null && !transform.continuous) TransformMetadata.Status.FINISHED else TransformMetadata.Status.STARTED
            )
        } else metadata.copy(lastUpdatedAt = Instant.now(), status = TransformMetadata.Status.STARTED)
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
