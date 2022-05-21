/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementException
import org.opensearch.indexmanagement.snapshotmanagement.preFixTimeStamp
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.smTransitions
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import org.opensearch.indexmanagement.snapshotmanagement.indexMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.upsert
import org.opensearch.indexmanagement.snapshotmanagement.smJobIdToPolicyName
import java.time.Instant.now

class SMStateMachine(
    val client: Client,
    val job: SMPolicy,
    var metadata: SMMetadata
) : StateMachine() {

    val log: Logger = LogManager.getLogger("$javaClass [${smJobIdToPolicyName(job.id)}]")

    override var currentState: SMState = metadata.currentState.also {
        log.info("Current state: [${metadata.currentState}].")
    }

    override suspend fun next() {
        try {
            do {
                val nextStates = smTransitions[currentState]
                if (nextStates == null) {
                    // Unlikely to reach unless the metadata is tampered
                    log.error("No next states for current state [$currentState].")
                    return
                }

                var result: ExecutionResult = ExecutionResult.Stay()
                for (nextState in nextStates) {
                    val prevState = currentState
                    currentState = nextState
                    log.info("sm dev: Start executing $currentState.")
                    result = nextState.instance.execute(this)
                    when (result) {
                        is ExecutionResult.Next -> {
                            log.info("State [$currentState]'s execution finished, will execute its next state.")
                            updateMetadata(result.metadataToSave.copy(currentState=currentState))
                            // break the nextStates loop so to avoid execute other lateral states
                            break
                        }
                        is ExecutionResult.Stay -> {
                            log.info("State [$currentState]'s execution not finished, will stay.")
                            val metadataToSave = result.metadataToSave
                            metadataToSave?.let { updateMetadata(metadataToSave.copy(currentState=prevState)) }
                            // can still execute other lateral states if exists
                        }
                        is ExecutionResult.Failure -> {
                            val ex = result.ex
                            log.error("Caught exception while executing state [$currentState], will skip to the START state.", ex)

                            val userMessage = preFixTimeStamp(SnapshotManagementException(ex).message)
                            val info = metadata.info.upsert(
                                "exception" to userMessage
                            )
                            val metadataToSave = SMMetadata.Builder(metadata)
                                .reset(result.resetType)
                                .info(info)
                                .build()
                            updateMetadata(metadataToSave)

                            // TODO error notification
                            break
                        }
                    }
                }
                if (result !is ExecutionResult.Next) {
                    // Only Next result requires checking continuous flag
                    break
                }
            } while (currentState.instance.continuous)
        } catch (ex: Exception) {
            // For update metadata exception, we won't try to update metadata again
            log.error("Snapshot management uncaught runtime exception.", ex)
        }
    }

    /**
     * Update job running status metadata
     *
     * In one lifecycle of this context object, there could be multiple
     *  metadata update operations, so we keep the seqNo and priTerm.
     *
     * If experienced with any exception during update metadata, state machine stuck
     * Indexing exception will be caught and logged in [SMStateMachine.next]
     */
    private var metadataSeqNo: Long = metadata.seqNo
    private var metadataPrimaryTerm: Long = metadata.primaryTerm
    private suspend fun updateMetadata(md: SMMetadata) {
        // TODO SM before update metadata, check if next execution time is earlier than now()
        //  if so we should update it next execution time to keep it up to date

        // TODO SM retry policy for update metadata
        val res = client.indexMetadata(md, job.id, metadataSeqNo, metadataPrimaryTerm)

        metadataSeqNo = res.seqNo
        metadataPrimaryTerm = res.primaryTerm
        metadata = md

        // TODO SM save a copy to history
    }

    /**
     * Handle the policy change before job running
     *
     * Currently, only handle schedule change in policy.
     */
    suspend fun handlePolicyChange(): SMStateMachine {
        if (job.seqNo > metadata.policySeqNo || job.primaryTerm > metadata.policyPrimaryTerm) {
            val metadataToSave = SMMetadata.Builder(metadata)
                .policyVersion(job.seqNo, job.primaryTerm)
                .nextCreationTime(getNextExecutionTime(job.creation.schedule, now()))
                .nextDeletionTime(getNextExecutionTime(job.deletion.schedule, now()))
                .build()
            updateMetadata(metadataToSave)
        }
        return this
    }
}
