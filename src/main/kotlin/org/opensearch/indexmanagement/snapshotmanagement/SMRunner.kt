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
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.SMStateMachine
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
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
        SMRunner.client = client
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        log.info("Snapshot management running job: $job")

        launch {
            job as SMPolicy

            var metadata = client.getMetadata(job.id)
            if (metadata == null) {
                metadata = initMetadata(job)
                metadata ?: return@launch
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
            currentState = SMState.START,
            creation = SMMetadata.Creation(
                SMMetadata.Trigger(getNextExecutionTime(job.creation.schedule, now()))
            ),
            deletion = SMMetadata.Deletion(
                SMMetadata.Trigger(getNextExecutionTime(job.deletion.schedule, now()))
            ),
        )
        log.info("Initializing metadata [$metadata] for policy [${job.id}].")
        try {
            val res = client.indexMetadata(metadata, job.id, create = true)
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
