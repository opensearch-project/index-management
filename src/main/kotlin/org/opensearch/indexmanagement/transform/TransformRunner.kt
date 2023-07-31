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
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.xcontent.NamedXContentRegistry
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
import org.opensearch.indexmanagement.transform.util.TransformContext
import org.opensearch.indexmanagement.transform.util.TransformLockManager
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
        val transformContext = TransformContext(TransformLockManager(transform, context))
        val transformLockManager = transformContext.transformLockManager
        transformLockManager.acquireLockForScheduledJob()

        var currentMetadata = metadata

        // The global checkpoint is the maximum seqNo synced across per primary shard and its replicas
        // This is only needed for continuous transform
        var newGlobalCheckpoints: Map<ShardId, Long>? = null
        // The time when new checkpoint was generated in continuous transform
        var newGlobalCheckpointTime: Instant? = null
        val transformProcessedBucketLog = TransformProcessedBucketLog()
        var bucketsToTransform = BucketsToTransform(HashSet(), metadata)
        try {
            do {
                when {
                    transformLockManager.lock == null -> {
                        logger.warn("Cannot acquire lock for transform job ${transform.id}")
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

                        // If date was used in term query, generate target date field mapping and store it in transform context
                        transformContext.setTargetDateFieldMappings(TargetIndexMappingService.getTargetMappingsForDates(transform))

                        if (transform.continuous) {
                            // For continuous transform, we first populate the shards to search
                            if (bucketsToTransform.shardsToSearch == null) {
                                // Note the timestamp when we got the shard global checkpoints to the user may know what data is included
                                newGlobalCheckpointTime = Instant.now()
                                newGlobalCheckpoints = transformSearchService.getShardsGlobalCheckpoint(transform.sourceIndex)
                                bucketsToTransform = bucketsToTransform.initializeShardsToSearch(
                                    metadata.shardIDToGlobalCheckpoint, newGlobalCheckpoints
                                )
                            }
                            // currentShardToSearch is the iterative pointer to the unprocessed shardsToSearch
                            if (bucketsToTransform.currentShardToSearch != null) {
                                // Find out the modified buckets for the current shard
                                bucketsToTransform = getBucketsToTransformIteration(transform, bucketsToTransform, transformContext).also {
                                    currentMetadata = it.metadata
                                }
                                // Filter out the already processed buckets from modified buckets
                                val modifiedBuckets = bucketsToTransform.modifiedBuckets.filter {
                                    transformProcessedBucketLog.isNotProcessed(it)
                                }.toMutableSet()
                                // Recompute modified buckets and update them in targetIndex
                                currentMetadata = recomputeModifiedBuckets(transform, currentMetadata, modifiedBuckets, transformContext)
                                // Add processed buckets to 'processed set' so that we don't try to reprocess them again
                                // It's very likely to have duplicated modified buckets from different shards
                                transformProcessedBucketLog.addBuckets(modifiedBuckets.toList())

                                currentMetadata = transformMetadataService.writeMetadata(currentMetadata, true)
                                bucketsToTransform = bucketsToTransform.copy(metadata = currentMetadata)
                            }
                        } else {
                            // Computes buckets from source index and stores them in targetIndex as transform docs
                            currentMetadata = computeBucketsIteration(transform, currentMetadata, transformContext)
                            // Update TransformMetadata
                            currentMetadata = transformMetadataService.writeMetadata(currentMetadata, true)
                        }
                        // Renew lock after every loop of transform
                        transformLockManager.renewLockForScheduledJob()
                    }
                }
            } while (bucketsToTransform.currentShardToSearch != null || currentMetadata.afterKey != null)
        } catch (e: Exception) {
            logger.error("Failed to execute the transform job [${transform.id}] because of exception [${e.localizedMessage}]", e)
            currentMetadata = currentMetadata.copy(
                lastUpdatedAt = Instant.now(),
                status = TransformMetadata.Status.FAILED,
                failureReason = e.localizedMessage
            )
        } finally {
            transformLockManager.lock?.let {
                // Update the global checkpoint for continuous transform only after this job run finishes successfully
                if (transform.continuous && currentMetadata.status != TransformMetadata.Status.FAILED) {
                    currentMetadata = currentMetadata.copy(
                        shardIDToGlobalCheckpoint = newGlobalCheckpoints,
                        continuousStats = ContinuousTransformStats(newGlobalCheckpointTime, null)
                    )
                    transformMetadataService.writeMetadata(currentMetadata, true)
                }

                // If this is a one time transform or continuous transform failed, disable the job
                if (!transform.continuous || currentMetadata.status == TransformMetadata.Status.FAILED) {
                    logger.info("Disabling the transform job ${transform.id}")
                    updateTransform(transform.copy(enabled = false, enabledAt = null))
                }

                transformLockManager.releaseLockForScheduledJob()
            }
        }
    }

    /**
     * Do transform search on modified documents for current shard to find out modified buckets
     */
    private suspend fun getBucketsToTransformIteration(
        transform: Transform,
        bucketsToTransform: BucketsToTransform,
        transformContext: TransformContext
    ): BucketsToTransform {
        var currentBucketsToTransform = bucketsToTransform
        val currentShard = bucketsToTransform.currentShardToSearch
        // Clear modified buckets from previous iteration
        currentBucketsToTransform.modifiedBuckets.clear()

        if (currentShard != null) {
            val shardLevelModifiedBuckets = withTransformSecurityContext(transform) {
                transformSearchService.getShardLevelModifiedBuckets(
                    transform,
                    currentBucketsToTransform.metadata.afterKey,
                    currentShard,
                    transformContext
                )
            }
            currentBucketsToTransform.modifiedBuckets.addAll(shardLevelModifiedBuckets.modifiedBuckets)
            val mergedSearchTime = currentBucketsToTransform.metadata.stats.searchTimeInMillis +
                shardLevelModifiedBuckets.searchTimeInMillis
            currentBucketsToTransform = currentBucketsToTransform.copy(
                metadata = currentBucketsToTransform.metadata.copy(
                    stats = currentBucketsToTransform.metadata.stats.copy(
                        pagesProcessed = currentBucketsToTransform.metadata.stats.pagesProcessed + 1, // TODO reading is here the right place to increment the pagesProcessed?
                        searchTimeInMillis = mergedSearchTime
                    ),
                    afterKey = shardLevelModifiedBuckets.afterKey
                ),
                currentShardToSearch = currentShard
            )
        }
        // If finished with this shard, go to the next
        if (currentBucketsToTransform.metadata.afterKey == null) {
            val shardsToSearch = currentBucketsToTransform.shardsToSearch
            currentBucketsToTransform = if (shardsToSearch?.hasNext() == true) {
                currentBucketsToTransform.copy(currentShardToSearch = shardsToSearch.next())
            } else {
                currentBucketsToTransform.copy(currentShardToSearch = null)
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
    private suspend fun computeBucketsIteration(
        transform: Transform,
        metadata: TransformMetadata,
        transformContext: TransformContext
    ): TransformMetadata {

        val transformSearchResult = withTransformSecurityContext(transform) {
            transformSearchService.executeCompositeSearch(
                transform,
                metadata.afterKey,
                null,
                transformContext
            )
        }
        val indexTimeInMillis = withTransformSecurityContext(transform) {
            transformIndexer.index(transform.targetIndex, transformSearchResult.docsToIndex, transformContext)
        }
        val afterKey = transformSearchResult.afterKey
        val stats = transformSearchResult.stats
        val updatedStats = stats.copy(
            pagesProcessed = stats.pagesProcessed,
            indexTimeInMillis = stats.indexTimeInMillis + indexTimeInMillis,
            documentsIndexed = transformSearchResult.docsToIndex.size.toLong()
        )
        return metadata.mergeStats(updatedStats).copy(
            afterKey = afterKey,
            lastUpdatedAt = Instant.now(),
            status = if (afterKey == null) TransformMetadata.Status.FINISHED else TransformMetadata.Status.STARTED
        )
    }

    private suspend fun recomputeModifiedBuckets(
        transform: Transform,
        metadata: TransformMetadata,
        modifiedBuckets: MutableSet<Map<String, Any>>,
        transformContext: TransformContext
    ): TransformMetadata {
        val updatedMetadata = if (modifiedBuckets.isNotEmpty()) {
            val transformSearchResult = withTransformSecurityContext(transform) {
                transformSearchService.executeCompositeSearch(transform, null, modifiedBuckets, transformContext)
            }
            val indexTimeInMillis = withTransformSecurityContext(transform) {
                transformIndexer.index(transform.targetIndex, transformSearchResult.docsToIndex, transformContext)
            }
            val stats = transformSearchResult.stats
            val updatedStats = stats.copy(
                pagesProcessed = if (transform.continuous) 0 else stats.pagesProcessed,
                indexTimeInMillis = stats.indexTimeInMillis + indexTimeInMillis,
                documentsIndexed = transformSearchResult.docsToIndex.size.toLong()
            )
            metadata.mergeStats(updatedStats).copy(
                lastUpdatedAt = Instant.now(),
                status = TransformMetadata.Status.STARTED
            )
        } else {
            metadata.copy(lastUpdatedAt = Instant.now(), status = TransformMetadata.Status.STARTED)
        }
        return updatedMetadata
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
