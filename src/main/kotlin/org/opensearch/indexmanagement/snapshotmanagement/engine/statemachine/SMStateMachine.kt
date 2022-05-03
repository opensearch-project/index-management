/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.index.IndexResponse
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.StateMachineException.ExceptionCode
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.StateMachineException.Companion.getUserMsg
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.smTransitions
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import org.opensearch.indexmanagement.snapshotmanagement.indexMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.upsert
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import java.time.Instant.now

class SMStateMachine(
    val client: Client,
    val job: SMPolicy,
    var metadata: SMMetadata
) : StateMachine() {
    val log: Logger = LogManager.getLogger("$javaClass [${smDocIdToPolicyName(job.id)}]")

    override var currentState: SMState = metadata.currentState.also {
        log.info("Set state machine current state from metadata: ${metadata.currentState}")
    }

    private var metadataSeqNo: Long = metadata.seqNo
    private var metadataPrimaryTerm: Long = metadata.primaryTerm

    override suspend fun next() {
        try {
            do {
                val nextStates = smTransitions[currentState]
                if (nextStates == null) {
                    log.error("Cannot find the next states")
                    return
                }

                var result: ExecutionResult = ExecutionResult.Stay()
                for (nextState in nextStates) {
                    currentState = nextState
                    log.info("Start executing $currentState")
                    result = nextState.instance.execute(this)
                    when (result) {
                        is ExecutionResult.Next -> {
                            log.info("[$currentState] execution fully finished, moving to [$currentState].")
                            updateMetadata(result.metadataToSave)
                            break // break the nextStates loop
                        }
                        is ExecutionResult.Stay -> {
                            log.info("[$currentState] execution not fully finished.")
                            if (result.metadataToSave != null) updateMetadata(result.metadataToSave!!)
                        }
                        is ExecutionResult.Failure -> {
                            val ex = result.ex
                            log.error("Caught exception while executing [$currentState], skipping to next execution.")
                            var userMessage: String
                            if (ex is StateMachineException) {
                                userMessage = ex.getUserMsg()
                                log.error("Exception details: $userMessage.")
                            } else {
                                userMessage = StateMachineException(ex).getUserMsg()
                                log.error("Exception details: ", ex)
                            }
                            val metadataToSave = metadata.copy(
                                currentState = SMState.START,
                                creation = metadata.creation.copy(
                                    started = null,
                                    finished = null,
                                ),
                                deletion = metadata.deletion.copy(
                                    started = null,
                                    startedTime = null,
                                ),
                                info = metadata.info.upsert(
                                    "exception" to userMessage
                                )
                            )
                            updateMetadata(metadataToSave)

                            // TODO error notification
                            break
                        }
                    }
                }
                if (result !is ExecutionResult.Next) {
                    break // break the do while loop
                }
            } while (currentState.instance.continuous)
        } catch (ex: Exception) {
            // update metadata exception
            log.error("Uncaught exception:", ex)
        }
    }

    /**
     * In one lifecycle of this context object, there could be multiple
     * metadata update operations, so we keep seqNo and priTerm in this object.
     *
     * If any failure during update metadata, we can only log out the error.
     * State machine continues to execute when metadata update is functional.
     */
    suspend fun updateMetadata(md: SMMetadata) {
        val res: IndexResponse
        try {
            res = client.indexMetadata(md, job.id, metadataSeqNo, metadataPrimaryTerm)
        } catch (ex: Exception) {
            throw StateMachineException(ExceptionCode.METADATA_UPDATE, ex)
        }

        metadataSeqNo = res.seqNo
        metadataPrimaryTerm = res.primaryTerm
        metadata = md

        // TODO save a copy to history
    }

    /**
     * Supposed to be used during [SMStateMachine] initialization
     */
    suspend fun handlePolicyChange(): SMStateMachine {
        if (job.seqNo > metadata.policySeqNo || job.primaryTerm > metadata.policyPrimaryTerm) {
            val metadataToSave = metadata.copy(
                policySeqNo = job.seqNo,
                policyPrimaryTerm = job.primaryTerm,
                creation = SMMetadata.Creation(
                    SMMetadata.Trigger(getNextExecutionTime(job.creation.schedule, now()))
                ),
                deletion = SMMetadata.Deletion(
                    SMMetadata.Trigger(getNextExecutionTime(job.deletion.schedule, now()))
                ),
            )
            updateMetadata(metadataToSave)
        }
        return this
    }
}
