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
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMState
import org.opensearch.indexmanagement.snapshotmanagement.getMetadata
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import org.opensearch.indexmanagement.snapshotmanagement.indexMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
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
        this.client = client
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        log.info("Snapshot management running job: $job")

        launch {
            job as SMPolicy

            var metadata = client.getMetadata(job.name)
            if (metadata == null) {
                metadata = initMetadata(job)
                if (metadata == null) return@launch
            }

            val stateMachineContext = SMStateMachine(client, job, metadata)
            stateMachineContext
                .handlePolicyChange()
                .next()
        }
    }

    private suspend fun initMetadata(job: SMPolicy): SMMetadata? {
        val metadata = SMMetadata(
            policySeqNo = job.seqNo,
            policyPrimaryTerm = job.primaryTerm,
            currentState = SMState.FINISHED,
            creation = SMMetadata.Creation(
                SMMetadata.Trigger(getNextExecutionTime(job.creation.schedule, now()))
            ),
            deletion = SMMetadata.Deletion(
                SMMetadata.Trigger(getNextExecutionTime(job.deletion.schedule, now()))
            ),
        )
        log.info("Initializing metadata [$metadata] for policy [${job.policyName}].")
        try {
            val res = client.indexMetadata(metadata, job.policyName, create = true)
            if (res.status() != RestStatus.CREATED) {
                log.error("Metadata initialization response status is ${res.status()}, expecting CREATED 201.")
                return null
            }
        } catch (e: Exception) {
            log.error("Caught exception while initializing SM metadata.", e)
            return null
        }
        return metadata
    }
}
