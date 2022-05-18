/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.index.IndexResponse
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.SMMetadataBuilder
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
import org.opensearch.indexmanagement.snapshotmanagement.smJobIdToPolicyName
import java.time.Instant.now

class SMStateMachine(
    val client: Client,
    val job: SMPolicy,
    var metadata: SMMetadata
) : StateMachine() {

    val log: Logger = LogManager.getLogger("$javaClass [${smJobIdToPolicyName(job.id)}]")

    override var currentState: SMState = metadata.currentState.also {
        log.info("Snapshot management state machine current state set to [${metadata.currentState}].")
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
                    currentState = nextState
                    log.info("sm dev: Start executing $currentState.")
                    result = nextState.instance.execute(this)
                    when (result) {
                        is ExecutionResult.Next -> {
                            log.info("[$currentState]'s execution fully finished.")
                            updateMetadata(result.metadataToSave)
                            // break the nextStates loop so to avoid execute other lateral states
                            break
                        }
                        is ExecutionResult.Stay -> {
                            log.info("[$currentState] execution not fully finished.")
                            val metadataToSave = result.metadataToSave
                            metadataToSave?.let { updateMetadata(metadataToSave) }
                            // can still execute other lateral states if exists
                        }
                        is ExecutionResult.Failure -> {
                            val ex = result.ex
                            log.error("Caught exception while executing [$currentState], will skip to the START state.")
                            var userMessage: String
                            if (ex is StateMachineException) {
                                userMessage = ex.getUserMsg()
                                log.error("Exception details: $userMessage.")
                            } else {
                                userMessage = StateMachineException(ex).getUserMsg()
                                log.error("Exception details: ", ex)
                            }

                            val info = metadata.info.upsert(
                                "exception" to userMessage
                            )
                            val metadataToSave = SMMetadataBuilder(metadata)
                                .currentState(SMState.START)
                                .startedCreation(null)
                                .finishedCreation(null)
                                .startedDeletion(null)
                                .deletionStartTime(null)
                                .info(info)
                                .build()

                            log.info("sm dev: will metadata be changed? $metadata")
                            log.info("sm dev: metadataToSave: $metadataToSave")
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
            // update metadata exception will be caught here
            log.error("Uncaught exception:", ex)
        }
    }

    /**
     * Update running job metadata in state machine
     *
     * In one lifecycle of this context object, there could be multiple
     * metadata update operations, so we keep the seqNo and priTerm.
     *
     * If any failure during update metadata, state machine stuck,
     * and error is logged.
     */
    private var metadataSeqNo: Long = metadata.seqNo
    private var metadataPrimaryTerm: Long = metadata.primaryTerm
    private suspend fun updateMetadata(md: SMMetadata) {
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
     * Handle the policy change before job running
     *
     * Currently, only handle schedule change in policy.
     */
    suspend fun handlePolicyChange(): SMStateMachine {
        if (job.seqNo > metadata.policySeqNo || job.primaryTerm > metadata.policyPrimaryTerm) {
            val metadataToSave = metadata.copy(
                policySeqNo = job.seqNo,
                policyPrimaryTerm = job.primaryTerm,
                creation = SMMetadata.Creation(
                    SMMetadata.Trigger(
                        time = getNextExecutionTime(job.creation.schedule, now())
                    )
                ),
                deletion = SMMetadata.Deletion(
                    SMMetadata.Trigger(
                        time = getNextExecutionTime(job.deletion.schedule, now())
                    )
                ),
            )
            updateMetadata(metadataToSave)
        }
        return this
    }
}
