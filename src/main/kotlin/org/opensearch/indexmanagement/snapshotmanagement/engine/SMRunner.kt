/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.client.Client
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.getMetadata
import org.opensearch.indexmanagement.snapshotmanagement.indexMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.util.acquireLockForScheduledJob
import org.opensearch.indexmanagement.util.releaseLockForScheduledJob
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.ScheduledJobRunner
import org.opensearch.rest.RestStatus
import java.time.Instant.now

object SMRunner :
    ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("snapshot_management_runner")) {

    private val log = LogManager.getLogger(javaClass)

    lateinit var client: Client

    fun init(client: Client): SMRunner {
        SMRunner.client = client
        return this
    }

    private val backoffPolicy: BackoffPolicy = BackoffPolicy.exponentialBackoff(
        TimeValue.timeValueMillis(1000L), 3
    )

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        log.info("sm dev: Snapshot management running job: $job")

        if (job !is SMPolicy) {
            throw IllegalArgumentException("Received invalid job type [${job.javaClass.simpleName}] with id [${context.jobId}].")
        }

        launch {
            val lock = acquireLockForScheduledJob(job, context, backoffPolicy)
            if (lock == null) {
                log.warn("Cannot acquire lock for snapshot management job ${job.id}")
                return@launch
            }

            var metadata = client.getMetadata(job)
            if (metadata == null) {
                metadata = initMetadata(job)
                metadata ?: return@launch
            }

            // TODO SM state machine logic

            if (!releaseLockForScheduledJob(context, lock)) {
                log.debug("Could not release lock [${lock.lockId}] for ${job.id}.")
            }
        }
    }

    /**
     * Initialize snapshot management job run metadata
     *
     * @return null indicates indexing metadata failed
     */
    private suspend fun initMetadata(job: SMPolicy): SMMetadata? {
        val initMetadata = SMMetadata(
            id = job.id,
            policySeqNo = job.seqNo,
            policyPrimaryTerm = job.primaryTerm,
            currentState = SMState.START,
            creation = SMMetadata.Creation(
                SMMetadata.Trigger(
                    time = job.creation.schedule.getNextExecutionTime(now())
                )
            ),
            deletion = SMMetadata.Deletion(
                SMMetadata.Trigger(
                    time = job.deletion.schedule.getNextExecutionTime(now())
                )
            ),
        )
        log.info("Initializing metadata [$initMetadata] for job [${job.id}].")
        try {
            val res = client.indexMetadata(initMetadata, job.id, create = true)
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
}
