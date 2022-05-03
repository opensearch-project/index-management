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
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.State.ExecutionResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine.StateMachineException.Companion.getUserMsg
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import org.opensearch.indexmanagement.snapshotmanagement.indexMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.upsert
import java.time.Instant.now

/**
 * Context for state machine execution
 *
 * In one lifecycle of this context object, there could be multiple
 * metadata update operations
 */
class SMStateMachine(
    val client: Client,
    val job: SMPolicy,
    var metadata: SMMetadata
) : StateMachine() {
    val log: Logger = LogManager.getLogger("${SMStateMachine::class.java.name} [${job.policyName}]")

    override var currentState: SMState = metadata.currentState.also {
        log.info("Set state machine current state from metadata: ${metadata.currentState}")
    }

    private var metadataSeqNo: Long = metadata.seqNo
    private var metadataPrimaryTerm: Long = metadata.primaryTerm

    override suspend fun next() {
        try {
            do {
                val nextStates = transitions[currentState]
                if (nextStates == null) {
                    log.error("Cannot find the next states")
                    return
                }

                var result: ExecutionResult = ExecutionResult.NotMet(false)
                for (nextState in nextStates) {
                    currentState = nextState
                    log.info("Start executing $currentState")
                    result = nextState.instance.execute(this)
                    when (result) {
                        is ExecutionResult.Next -> {
                            updateMetadata(result.md)
                            log.info("Moved to [$currentState].")
                            break // break the nextStates loop
                        }
                        is ExecutionResult.NotMet -> {
                            log.info("[$currentState] execution not fully finished.")
                            if (!result.cont) {
                                if (result.md != null) updateMetadata(result.md!!)
                                break
                            }
                        }
                        is ExecutionResult.Failure -> {
                            val ex = result.ex
                            log.error("Caught exception while executing [$currentState], skipping to next execution.", ex)
                            val userMessage = if (ex is StateMachineException) {
                                ex.getUserMsg()
                            } else {
                                StateMachineException(ex).getUserMsg()
                            }
                            log.info("User message: $userMessage")
                            val metadataToSave = metadata.copy(
                                currentState = SMState.START,
                                atomic = false,
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
            log.error(ex)
        }
    }

    /**
     * If any failure during update metadata, the best we can do is to log out error
     * and state machine can only continue execution when it can update metadata
     */
    suspend fun updateMetadata(md: SMMetadata) {
        val res: IndexResponse
        try {
            res = client.indexMetadata(md, job.policyName, metadataSeqNo, metadataPrimaryTerm)
        } catch (ex: Exception) {
            throw StateMachineException(ExceptionCode.METADATA_UPDATE, ex)
        }

        metadataSeqNo = res.seqNo
        metadataPrimaryTerm = res.primaryTerm
        metadata = md

        // TODO save a copy to history
    }

    /**
     * Handle policy change during [SMStateMachine] initialization
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
