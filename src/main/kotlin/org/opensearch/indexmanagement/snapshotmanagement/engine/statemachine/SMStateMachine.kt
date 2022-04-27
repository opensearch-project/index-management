/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.index.IndexResponse
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import org.opensearch.indexmanagement.snapshotmanagement.indexMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata.Companion.upsert
import org.opensearch.rest.RestStatus
import java.time.Instant.now

/**
 * Context for state machine execution
 */
class SMStateMachine(
    val client: Client,
    val job: SMPolicy,
    var metadata: SMMetadata
) : StateMachine {
    val log: Logger = LogManager.getLogger("${javaClass.name} [${job.policyName}]")

    var currentState: SMState

    private var metadataSeqNo: Long
    private var metadataPrimaryTerm: Long
    var metadataToSave: SMMetadata?
    init {
        log.info("Set state machine current state from metadata: ${metadata.currentState}")
        currentState = metadata.currentState

        metadataSeqNo = metadata.seqNo
        metadataPrimaryTerm = metadata.primaryTerm
        metadataToSave = metadata
    }

    override suspend fun next() {
        try {
            do {
                val nextStates = transitions[currentState]
                if (nextStates == null) {
                    log.error("Next state is null")
                    return
                }
                log.info("Next states: $nextStates")

                var executionEndState = false
                for (nextState in nextStates) {
                    currentState = nextState
                    log.info("Start executing $currentState")
                    executionEndState = nextState.instance.execute(this)
                    if (executionEndState) {
                        updateMetadata()
                        log.info("[$currentState] execution satisfied, move to this state.")
                        break // break the lateral loop, refer SMState.transitions
                    } else {
                        log.info("[$currentState] execution not satisfied, try next lateral state if exist.")
                    }
                }
                if (!executionEndState) {
                    log.info("[$nextStates] execution not satisfied, wait for next scheduled job run.")
                    break // break the vertical loop, refer SMState.transitions
                }
            } while (currentState.instance.continuous)
        } catch (ex: UpdateMetadataException) {
            //  TODO error notification
            log.error(ex)
        } catch (ex: StateMachineException) {
            log.error(ex)
            metadataToSave = metadata.copy(
                currentState = SMState.FINISHED,
                info = metadata.info.upsert(
                    "runtime_exception" to ex.message!!
                )
            )
            updateMetadata()
        } catch (ex: Exception) {
            log.error("Caught exception while executing $currentState", ex)
            metadataToSave = metadata.copy(
                currentState = SMState.FINISHED,
                info = metadata.info.upsert(
                    "runtime_exception" to genericRunTimeExMsg()
                )
            )
            updateMetadata()
        }
    }

    /**
     * If any failure during update metadata, the best we can do is to log out error
     * and state machine can only continue execution when it can update metadata
     *
     * @throws UpdateMetadataException
     */
    suspend fun updateMetadata() {
        val md = metadataToSave!!

        val res: IndexResponse
        try {
            res = client.indexMetadata(md, job.policyName, metadataSeqNo, metadataPrimaryTerm)
        } catch (ex: Exception) {
            throw UpdateMetadataException("Caught exception when updating snapshot management metadata.", ex)
        }
        if (res.status() != RestStatus.OK) {
            throw UpdateMetadataException("Metadata update returns ${res.status()}, expecting ${RestStatus.OK}.")
        }

        metadataSeqNo = res.seqNo
        metadataPrimaryTerm = res.primaryTerm
        metadata = md
        metadataToSave = null // force any metadataToSave to base on metadata
    }

    /**
     * Handle policy change during [SMStateMachine] initialization
     */
    suspend fun handlePolicyChange(): SMStateMachine {
        if (job.seqNo > metadata.policySeqNo || job.primaryTerm > metadata.policyPrimaryTerm) {
            metadataToSave = metadata.copy(
                policySeqNo = job.seqNo,
                policyPrimaryTerm = job.primaryTerm,
                creation = SMMetadata.Creation(
                    SMMetadata.Trigger(getNextExecutionTime(job.creation.schedule, now()))
                ),
                deletion = SMMetadata.Deletion(
                    SMMetadata.Trigger(getNextExecutionTime(job.deletion.schedule, now()))
                ),
            )
            updateMetadata()
        }
        return this
    }

    companion object {
        fun apiCallingMsg() = "Undetermined about whether the last snapshot has been created, moving to next."
        fun genericRunTimeExMsg() = "Caught exception while snapshot management running."
    }
}
