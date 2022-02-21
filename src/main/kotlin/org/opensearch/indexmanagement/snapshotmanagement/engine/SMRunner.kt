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
import org.opensearch.indexmanagement.snapshotmanagement.model.SM
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.ScheduledJobRunner
import java.time.Instant

object SMRunner :
    ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("SLMRunner")) {

    private val log = LogManager.getLogger(javaClass)

    lateinit var client: Client

    fun init(client: Client): SMRunner {
        this.client = client
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        log.info("sm run job: $job")

        launch {
            job as SM

            var metadata = client.getMetadata(job.name)

            if (metadata == null) {
                metadata = SMMetadata(
                    policySeqNo = job.seqNo,
                    policyPrimaryTerm = job.primaryTerm,
                    currentState = SMState.FINISHED.toString(),
                    nextCreationTime = getNextExecutionTime(job.createSchedule, Instant.now()),
                    nextDeletionTime = getNextExecutionTime(job.deleteSchedule, Instant.now()),
                )
                // TODO if exception throw here, will it stop the main process?
                if (!initMetadata(metadata, job.policyName)) return@launch
            }

            val stateMachineContext = SMStateMachine(client, job, metadata)
            if (job.seqNo != metadata.policySeqNo || job.primaryTerm != metadata.policyPrimaryTerm) {
                stateMachineContext.handlePolicyChange()
            }
            stateMachineContext.next()
        }
    }

    private suspend fun initMetadata(metadata: SMMetadata, id: String): Boolean {
        log.info("Init metadata [$metadata] for policy $id")
        val res = client.indexMetadata(metadata, id, create = true)
        log.info("Init metadata response: $res")
        // status code should be 201
        return true
    }
}
