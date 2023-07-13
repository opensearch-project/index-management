/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.indexstatemanagement.SkipExecution
import org.opensearch.indexmanagement.opensearchapi.IndexManagementSecurityContext
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.opensearchapi.withClosableContext
import org.opensearch.indexmanagement.rollup.action.get.GetRollupAction
import org.opensearch.indexmanagement.rollup.action.get.GetRollupRequest
import org.opensearch.indexmanagement.rollup.action.get.GetRollupResponse
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupAction
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupRequest
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupResponse
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.RollupJobValidationResult
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.RollupStats
import org.opensearch.indexmanagement.rollup.model.incrementStats
import org.opensearch.indexmanagement.rollup.model.mergeStats
import org.opensearch.indexmanagement.rollup.settings.RollupSettings
import org.opensearch.indexmanagement.rollup.util.RollupFieldValueExpressionResolver
import org.opensearch.indexmanagement.util.acquireLockForScheduledJob
import org.opensearch.indexmanagement.util.releaseLockForScheduledJob
import org.opensearch.indexmanagement.util.renewLockForScheduledJob
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.LockModel
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.ScheduledJobRunner
import org.opensearch.script.ScriptService
import org.opensearch.search.aggregations.bucket.composite.InternalComposite
import org.opensearch.threadpool.ThreadPool

@Suppress("TooManyFunctions")
object RollupRunner :
    ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("RollupRunner")) {

    private val logger = LogManager.getLogger(javaClass)
    private val backoffPolicy = BackoffPolicy.exponentialBackoff(
        TimeValue.timeValueMillis(RollupSettings.DEFAULT_ACQUIRE_LOCK_RETRY_DELAY),
        RollupSettings.DEFAULT_ACQUIRE_LOCK_RETRY_COUNT
    )

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var settings: Settings
    private lateinit var threadPool: ThreadPool
    private lateinit var rollupMapperService: RollupMapperService
    private lateinit var rollupIndexer: RollupIndexer
    private lateinit var rollupSearchService: RollupSearchService
    private lateinit var rollupMetadataService: RollupMetadataService
    private lateinit var clusterConfigurationProvider: SkipExecution

    fun registerClusterService(clusterService: ClusterService): RollupRunner {
        this.clusterService = clusterService
        return this
    }

    fun registerClient(client: Client): RollupRunner {
        this.client = client
        return this
    }

    fun registerNamedXContentRegistry(xContentRegistry: NamedXContentRegistry): RollupRunner {
        this.xContentRegistry = xContentRegistry
        return this
    }

    fun registerScriptService(scriptService: ScriptService): RollupRunner {
        this.scriptService = scriptService
        return this
    }

    fun registerSettings(settings: Settings): RollupRunner {
        this.settings = settings
        return this
    }

    fun registerThreadPool(threadPool: ThreadPool): RollupRunner {
        this.threadPool = threadPool
        return this
    }

    fun registerMapperService(rollupMapperService: RollupMapperService): RollupRunner {
        this.rollupMapperService = rollupMapperService
        return this
    }

    fun registerIndexer(rollupIndexer: RollupIndexer): RollupRunner {
        this.rollupIndexer = rollupIndexer
        return this
    }

    fun registerSearcher(rollupSearchService: RollupSearchService): RollupRunner {
        this.rollupSearchService = rollupSearchService
        return this
    }

    fun registerMetadataServices(
        rollupMetadataService: RollupMetadataService
    ): RollupRunner {
        this.rollupMetadataService = rollupMetadataService
        return this
    }

    fun registerClusterConfigurationProvider(clusterConfigurationProvider: SkipExecution): RollupRunner {
        this.clusterConfigurationProvider = clusterConfigurationProvider
        return this
    }

    fun registerConsumers(): RollupRunner {
        return this
    }

    @Suppress("ComplexMethod")
    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is Rollup) {
            throw IllegalArgumentException("Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}")
        }

        launch {
            var metadata: RollupMetadata? = null
            try {
                // We don't inject user role to manipulate documents saved in system index, like the rollup metadata here.
                if (job.metadataID != null) {
                    metadata = when (val getMetadataResult = rollupMetadataService.getExistingMetadata(job)) {
                        is MetadataResult.Success -> getMetadataResult.metadata
                        is MetadataResult.NoMetadata -> null
                        is MetadataResult.Failure ->
                            throw RollupMetadataException("Failed to get existing rollup metadata [${job.metadataID}]", getMetadataResult.cause)
                    }
                }
            } catch (e: RollupMetadataException) {
                // If the metadata was not retrieved, the exception will be logged and this job run will be a no-op
                logger.error(e.message, e.cause)
                return@launch
            }

            if (rollupSearchService.shouldProcessRollup(job, metadata)) {
                val lock = acquireLockForScheduledJob(job, context, backoffPolicy)
                if (lock == null) {
                    logger.debug("Could not acquire lock for ${job.id}")
                } else {
                    runRollupJob(job, context, lock)
                    releaseLockForScheduledJob(context, lock)
                }
            } else if (job.isEnabled) {
                // We are doing this if rollup doesn't need to be processed in this job run
                // as the job interval can be more frequent than rollup window, and we want to fail
                // validation as soon as possible
                // TODO should this and the one in runRollupJob be moved up before shouldProcessRollup?
                when (val jobValidity = isJobValid(job)) {
                    is RollupJobValidationResult.Invalid -> {
                        val lock = acquireLockForScheduledJob(job, context, backoffPolicy)
                        if (lock != null) {
                            setFailedMetadataAndDisableJob(job, jobValidity.reason)
                            logger.info("Set rollup [${job.id}] to failed state and disable the job [${job.id}]")
                            releaseLockForScheduledJob(context, lock)
                        }
                    }
                    else -> {}
                }
            }
        }
    }

    /**
     * TODO Scenarios:
     *  The rollup job is finished, but I (the user) want to redo it all again
     *  There is a rollup.metadataID and doc but there's no job in target index?
     *   -> index was deleted and recreated as rollup -> just recreate (but we would have to start over)? Or move to FAILED?
     */
    @Suppress("ReturnCount", "NestedBlockDepth", "ComplexMethod", "LongMethod", "ThrowsCount")
    private suspend fun runRollupJob(job: Rollup, context: JobExecutionContext, lock: LockModel) {
        var currLock = lock
        try {
            when (val jobValidity = isJobValid(job)) {
                is RollupJobValidationResult.Invalid -> {
                    logger.error("Invalid job [${job.id}]: [${jobValidity.reason}]")
                    setFailedMetadataAndDisableJob(job, jobValidity.reason)
                    return
                }
                is RollupJobValidationResult.Failure -> {
                    logger.error("Failed to validate [${job.id}]: [${jobValidity.message}]")
                    setFailedMetadataAndDisableJob(job, jobValidity.message)
                    return
                }
                else -> {}
            }

            var metadata = when (val initMetadataResult = rollupMetadataService.getOrInitMetadata(job)) {
                is MetadataResult.Success -> initMetadataResult.metadata
                is MetadataResult.NoMetadata -> {
                    logger.info("Init metadata NoMetadata returning early")
                    return
                } // No-op this execution
                is MetadataResult.Failure ->
                    throw RollupMetadataException("Failed to initialize rollup metadata", initMetadataResult.cause)
            }

            if (metadata.status == RollupMetadata.Status.FAILED) {
                logger.info("Metadata status is FAILED, disabling job $metadata")
                disableJob(job, metadata)
                return
            }

            // If metadata was created for the first time, update rollup job with metadata id
            var currJob = job
            if (currJob.metadataID == null && metadata.status == RollupMetadata.Status.INIT) {
                when (val updateRollupJobResult = updateRollupJob(currJob.copy(metadataID = metadata.id), metadata)) {
                    is RollupJobResult.Success -> currJob = updateRollupJobResult.rollup
                    is RollupJobResult.Failure -> {
                        logger.error(
                            "Failed to update the rollup job [${currJob.id}] with metadata id [${metadata.id}]", updateRollupJobResult.cause
                        )
                        return // Exit runner early
                    }
                }
            }

            when (
                val result = withClosableContext(
                    IndexManagementSecurityContext(job.id, settings, threadPool.threadContext, job.user)
                ) {
                    rollupMapperService.attemptCreateOrValidateTargetIndex(currJob, clusterConfigurationProvider.hasLegacyPlugin)
                }
            ) {
                is RollupJobValidationResult.Failure -> {
                    setFailedMetadataAndDisableJob(currJob, result.message, metadata)
                    return
                }
                is RollupJobValidationResult.Invalid -> {
                    setFailedMetadataAndDisableJob(currJob, result.reason, metadata)
                    return
                }
                else -> {}
            }

            while (rollupSearchService.shouldProcessRollup(currJob, metadata)) {
                do {
                    try {
                        val rollupResult = when (
                            val rollupSearchResult = withClosableContext(
                                IndexManagementSecurityContext(job.id, settings, threadPool.threadContext, job.user)
                            ) {
                                rollupSearchService.executeCompositeSearch(currJob, metadata)
                            }
                        ) {
                            is RollupSearchResult.Success -> {
                                val compositeRes: InternalComposite = rollupSearchResult.searchResponse.aggregations.get(currJob.id)
                                metadata = metadata.incrementStats(rollupSearchResult.searchResponse, compositeRes)

                                when (
                                    val rollupIndexResult = withClosableContext(
                                        IndexManagementSecurityContext(job.id, settings, threadPool.threadContext, job.user)
                                    ) {
                                        rollupIndexer.indexRollups(currJob, compositeRes)
                                    }
                                ) {
                                    is RollupIndexResult.Success -> RollupResult.Success(compositeRes, rollupIndexResult.stats)
                                    is RollupIndexResult.Failure -> RollupResult.Failure(rollupIndexResult.message, rollupIndexResult.cause)
                                }
                            }
                            is RollupSearchResult.Failure -> {
                                RollupResult.Failure(rollupSearchResult.message, rollupSearchResult.cause)
                            }
                        }

                        when (rollupResult) {
                            is RollupResult.Success -> {
                                metadata = rollupMetadataService.updateMetadata(
                                    currJob, metadata.mergeStats(rollupResult.stats), rollupResult.internalComposite
                                )

                                // TODO why get back job again here?
                                currJob = withClosableContext(
                                    IndexManagementSecurityContext(job.id, settings, threadPool.threadContext, null)
                                ) {
                                    client.suspendUntil { listener: ActionListener<GetRollupResponse> ->
                                        execute(GetRollupAction.INSTANCE, GetRollupRequest(currJob.id, null, "_local"), listener)
                                    }.rollup ?: error("Unable to get rollup job")
                                }
                            }
                            is RollupResult.Failure -> {
                                rollupMetadataService.updateMetadata(
                                    metadata.copy(status = RollupMetadata.Status.FAILED, failureReason = rollupResult.cause.message)
                                )
                            }
                        }

                        val renewedLock = renewLockForScheduledJob(context, currLock, backoffPolicy)
                        if (renewedLock == null) {
                            // If we fail to renew the lock it doesn't mean we need to permanently fail the job, we can just return early
                            // and let the next execution try to process the data from where this one left off
                            releaseLockForScheduledJob(context, currLock)
                            return
                        } else {
                            currLock = renewedLock
                        }
                    } catch (e: RollupMetadataException) {
                        // Rethrow this exception so it doesn't get consumed here
                        logger.info("RollupMetadataException being thrown", e)
                        throw e
                    } catch (e: Exception) {
                        // TODO: Should update metadata and disable job here instead of allowing the rollup to keep going
                        logger.error("Failed to rollup ", e)
                        releaseLockForScheduledJob(context, currLock)
                        return
                    }
                } while (metadata.afterKey != null)
            }

            if (!currJob.continuous) {
                if (listOf(RollupMetadata.Status.STOPPED, RollupMetadata.Status.FINISHED, RollupMetadata.Status.FAILED).contains(metadata.status)) {
                    disableJob(currJob, metadata)
                }
            }

            // If we have been constantly renewing the lock then the seqNo/primaryTerm will have changed
            // and the releaseLock call outside runRollupJob will fail, so release here with updatableLock
            // and outside just in case we returned early at a different point (attempting to release twice won't hurt)
            releaseLockForScheduledJob(context, currLock)
        } catch (e: RollupMetadataException) {
            // In most scenarios in the runner, the metadata will be used to communicate the result to the user
            // If change to the metadata itself fails, there is nothing else to relay state change
            // In these cases, the cause of the metadata operation will be logged here and the runner execution will exit
            logger.error(e.message, e.cause)
            releaseLockForScheduledJob(context, currLock)
        }
    }

    /**
     * Updates a rollup job.
     *
     * Takes in the metadata for the rollup job as well. This metadata should be create/updated
     * before passing it to this method as it will not create/update the metadata during normal conditions.
     *
     * However, in the case that the update to the rollup job fails, the provided metadata will be set to FAILED
     * status and updated to reflect the change.
     */
    private suspend fun updateRollupJob(job: Rollup, metadata: RollupMetadata): RollupJobResult {
        try {
            return withClosableContext(
                IndexManagementSecurityContext(job.id, settings, threadPool.threadContext, null)
            ) {
                val req = IndexRollupRequest(rollup = job, refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE)
                val res: IndexRollupResponse = client.suspendUntil { execute(IndexRollupAction.INSTANCE, req, it) }
                // TODO: Verify the seqNo/primterm got updated
                return@withClosableContext RollupJobResult.Success(res.rollup)
            }
        } catch (e: Exception) {
            // TODO: Catching general exceptions for now, can make more granular
            // Set metadata to failed since update to rollup job failed
            val errorMessage = "An error occurred when updating rollup job [${job.id}]"
            return when (val setFailedMetadataResult = rollupMetadataService.setFailedMetadata(job, errorMessage, metadata)) {
                is MetadataResult.Success -> RollupJobResult.Failure(errorMessage, e)
                // If the metadata update failed as well, throw an exception to end the runner execution
                is MetadataResult.Failure ->
                    throw RollupMetadataException(setFailedMetadataResult.message, setFailedMetadataResult.cause)
                // Should not get NoMetadata here
                is MetadataResult.NoMetadata ->
                    throw RollupMetadataException("Unexpected state when updating metadata", null)
            }
        }
    }

    /**
     * Validate source_index and target_index before the job actually runs.
     */
    @Suppress("ReturnCount", "ComplexMethod")
    private suspend fun isJobValid(job: Rollup): RollupJobValidationResult {
        return withClosableContext(
            IndexManagementSecurityContext(job.id, settings, threadPool.threadContext, job.user)
        ) {
            var metadata: RollupMetadata? = null
            if (job.metadataID != null) {
                metadata = when (val getMetadataResult = rollupMetadataService.getExistingMetadata(job)) {
                    is MetadataResult.Success -> getMetadataResult.metadata
                    is MetadataResult.NoMetadata -> null
                    is MetadataResult.Failure ->
                        throw RollupMetadataException("Failed to get existing rollup metadata [${job.metadataID}]", getMetadataResult.cause)
                }
            }
            logger.info("Fetched metadata for rollup job [${job.id}] to validate: $metadata")

            logger.debug("Validating source index [${job.sourceIndex}] for rollup job [${job.id}]")
            when (val sourceIndexValidationResult = rollupMapperService.isSourceIndexValid(job)) {
                is RollupJobValidationResult.Valid -> {
                } // No action taken when valid
                else -> return@withClosableContext sourceIndexValidationResult
            }

            // Only validate target index if there is metadata document in the rollup, because metadata should be created before target index
            if (metadata != null) {
                logger.debug("Attempting to validate target index [${job.targetIndex}] for rollup job [${job.id}]")
                val targetIndexResolvedName = RollupFieldValueExpressionResolver.resolve(job, job.targetIndex)
                val validationResult =
                    rollupMapperService.validateTargetIndex(job, targetIndexResolvedName, clusterConfigurationProvider.hasLegacyPlugin)
                when (validationResult) {
                    is RollupJobValidationResult.Failure -> logger.error(validationResult.message)
                    is RollupJobValidationResult.Invalid -> logger.error(validationResult.reason)
                    else -> {}
                }
                return@withClosableContext validationResult
            }

            return@withClosableContext RollupJobValidationResult.Valid
        }
    }

    /**
     * Sets a failed metadata (updating an existing metadata if provided, otherwise creating a new one) and disables the job.
     *
     * Returns true if disabling the job was successful. If any metadata operations fail along the way, RollupMetadataException
     * is thrown to be caught by the runner.
     */
    private suspend fun setFailedMetadataAndDisableJob(job: Rollup, reason: String, existingMetadata: RollupMetadata? = null): Boolean {
        val updatedMetadata = when (val setFailedMetadataResult = rollupMetadataService.setFailedMetadata(job, reason, existingMetadata)) {
            is MetadataResult.Success -> setFailedMetadataResult.metadata
            is MetadataResult.Failure ->
                throw RollupMetadataException(setFailedMetadataResult.message, setFailedMetadataResult.cause)
            // Should not get NoMetadata here
            is MetadataResult.NoMetadata ->
                throw RollupMetadataException("Unexpected state when setting failed metadata", null)
        }

        return disableJob(job, updatedMetadata)
    }

    /**
     * Disables a given job. Also takes in an existing metadata to replace the metadataID of the job
     * if this was a newly created metadata.
     *
     * This method will return true or false based on whether or not updating the job was successful. When
     * calling this method, it can be assumed that when the response is false, the failed metadata was updated
     * to relay the failure. If the metadata update failed, a RollupMetadataException would be thrown which gets
     * caught in the runner so that case does not need to be explicitly handled by the caller of the method.
     *
     * Note: Set metadata to failed and ensure it's updated before passing it to this method because it
     * will not update the metadata (unless updateRollupJob job fails).
     */
    private suspend fun disableJob(job: Rollup, metadata: RollupMetadata): Boolean {
        val updatedRollupJob = if (metadata.id != job.metadataID) {
            job.copy(metadataID = metadata.id, enabled = false, jobEnabledTime = null)
        } else {
            job.copy(enabled = false, jobEnabledTime = null)
        }

        return when (val updateRollupJobResult = updateRollupJob(updatedRollupJob, metadata)) {
            is RollupJobResult.Success -> true
            is RollupJobResult.Failure -> {
                logger.error("Failed to disable rollup job [${job.id}]", updateRollupJobResult.cause)
                false
            }
        }
    }
}

sealed class RollupJobResult {
    data class Success(val rollup: Rollup) : RollupJobResult()
    data class Failure(val message: String = "An error occurred for rollup job", val cause: Exception) : RollupJobResult()
}

sealed class RollupResult {
    data class Success(val internalComposite: InternalComposite, val stats: RollupStats) : RollupResult()
    data class Failure(val message: String = "An error occurred while rolling up", val cause: Exception) : RollupResult()
}
