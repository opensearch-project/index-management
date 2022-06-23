/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.client.Client
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.cluster.health.ClusterHealthStatus
import org.opensearch.cluster.health.ClusterStateHealth
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.creationTransitions
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.deletionTransitions
import org.opensearch.indexmanagement.util.acquireLockForScheduledJob
import org.opensearch.indexmanagement.util.releaseLockForScheduledJob
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.ScheduledJobRunner
import org.opensearch.rest.RestStatus
import org.opensearch.threadpool.ThreadPool
import java.time.Instant.now

object SMRunner :
    ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("snapshot_management_runner")) {

    private val log = LogManager.getLogger(javaClass)

    private lateinit var client: Client
    private lateinit var indicesManager: IndexManagementIndices
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var settings: Settings

    fun init(
        client: Client,
        threadPool: ThreadPool,
        settings: Settings,
        indicesManager: IndexManagementIndices,
        clusterService: ClusterService,
    ): SMRunner {
        this.client = client
        this.threadPool = threadPool
        this.settings = settings
        this.indicesManager = indicesManager
        this.clusterService = clusterService
        return this
    }

    private val backoffPolicy: BackoffPolicy = BackoffPolicy.exponentialBackoff(
        TimeValue.timeValueMillis(1000L), 3
    )

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        log.debug("Snapshot management running job: $job")

        if (job !is SMPolicy) {
            throw IllegalArgumentException("Received invalid job type [${job.javaClass.simpleName}] with id [${context.jobId}].")
        }

        launch {
            val lock = acquireLockForScheduledJob(job, context, backoffPolicy)
            if (lock == null) {
                log.warn("Cannot acquire lock for snapshot management job ${job.policyName}")
                return@launch
            }

            if (ClusterStateHealth(clusterService.state()).status == ClusterHealthStatus.RED) {
                log.warn("Skipping current execution of ${job.policyName} because of red cluster health")
                return@launch
            }

            try {
                var metadata = try {
                    client.getSMMetadata(job.id)
                } catch (e: Exception) {
                    log.error("Failed to retrieve metadata before running ${job.policyName}", e)
                    return@launch
                }
                if (metadata == null) {
                    metadata = initMetadata(job)
                    metadata ?: return@launch
                }

                // creation, deletion workflow have to be executed sequentially,
                // because they are sharing the same metadata document.
                SMStateMachine(client, job, metadata, settings, threadPool, indicesManager)
                    .handlePolicyChange()
                    .currentState(metadata.creation.currentState)
                    .next(creationTransitions)
                    .apply {
                        val deleteMetadata = metadata.deletion
                        if (deleteMetadata != null) {
                            this.currentState(deleteMetadata.currentState)
                                .next(deletionTransitions)
                        }
                    }
            } finally {
                if (!releaseLockForScheduledJob(context, lock)) {
                    log.error("Could not release lock [${lock.lockId}] for ${job.id}.")
                }
            }
        }
    }

    /**
     * Initialize snapshot management job run metadata
     *
     * @return null indicates indexing metadata failed
     */
    @Suppress("ReturnCount")
    private suspend fun initMetadata(job: SMPolicy): SMMetadata? {
        val initMetadata = getInitialMetadata(job)
        log.info("Initializing metadata [$initMetadata] for [${job.policyName}].")
        try {
            // TODO SM more granular error checking
            val res = client.indexMetadata(
                initMetadata, job.id, create = true,
                seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
            )
            if (res.status() != RestStatus.CREATED) {
                log.error("Metadata initialization response status is ${res.status()}, expecting CREATED 201.")
                return null
            }
        } catch (e: Exception) {
            log.error("Caught exception while initializing SM metadata.", e)
            return null
        }
        return initMetadata
    }

    private fun getInitialMetadata(job: SMPolicy): SMMetadata {
        val now = now()
        return SMMetadata(
            id = smPolicyNameToMetadataDocId(smDocIdToPolicyName(job.id)),
            policySeqNo = job.seqNo,
            policyPrimaryTerm = job.primaryTerm,
            creation = SMMetadata.WorkflowMetadata(
                SMState.CREATION_START,
                SMMetadata.Trigger(
                    time = job.creation.schedule.getNextExecutionTime(now)
                )
            ),
            deletion = job.deletion?.let {
                SMMetadata.WorkflowMetadata(
                    SMState.DELETION_START,
                    SMMetadata.Trigger(
                        time = job.deletion.schedule.getNextExecutionTime(now)
                    )
                )
            },
        )
    }
}
