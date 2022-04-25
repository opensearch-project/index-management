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
import org.opensearch.rest.RestStatus
import java.time.Instant

/**
 * Context for state machine execution
 */
class SMStateMachine(
    val client: Client,
    val job: SMPolicy,
    var metadata: SMMetadata
) {
    val log: Logger = LogManager.getLogger("${javaClass.name} [${job.policyName}]")

    var currentState: SMState

    private var metadataSeqNo: Long
    private var metadataPrimaryTerm: Long
    var metadataToSave: SMMetadata?
    init {
        log.info("Set state machine current state from metadata: ${metadata.currentState}")
        currentState = SMState.valueOf(metadata.currentState)

        metadataSeqNo = metadata.seqNo
        metadataPrimaryTerm = metadata.primaryTerm
        metadataToSave = metadata
    }

    suspend fun next() {
        try {
            do {
                val nextStates = transitions[currentState]
                if (nextStates == null) {
                    log.info("Next state is null")
                    return
                }
                log.info("Next lateral states: $nextStates")

                var executionEndState = false
                for (nextState in nextStates) {
                    currentState = nextState
                    log.info("Start executing $currentState")
                    executionEndState = nextState.instance.execute(this)
                    if (executionEndState) {
                        updateMetadata()
                        log.info("[$currentState] execution satisfied, move to this state.")
                        break // break the lateral loop
                    } else {
                        log.info("[$currentState] execution not satisfied, try next lateral state if exist.")
                    }
                }
                if (!executionEndState) {
                    log.info("[$nextStates] execution not satisfied, wait for next scheduled job run.")
                    break // break the vertical loop
                }
                log.info("[$currentState] continuous flag: ${currentState.instance.continuous}")
            } while (currentState.instance.continuous)
        } catch (e: Exception) {
            // TODO state execution failed with some exception
            //  save to info, error notification; move to START state
            //  special handle the updateMetadata exception, don't updateMetadata again
            log.error("Failed executing state $currentState", e)
        }
    }

    /**
     * if any failure during update metadata, the best we can do is to log out error
     * and state machine can only continue execution when it can update metadata
     */
    suspend fun updateMetadata() {
        val md = metadataToSave ?: throw NullPointerException("Null metadataToSave")

        val res: IndexResponse
        try {
            res = client.indexMetadata(md, job.policyName, metadataSeqNo, metadataPrimaryTerm)
        } catch (ex: Exception) {
            log.error("")
            // TODO throw special exception for update metadata
            throw ex
        }

        if (res.status() != RestStatus.OK) {
            log.info("Update metadata call status code ${res.status()}")
            // TODO throw special exception for update metadata
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
                nextCreationTime = getNextExecutionTime(job.createSchedule, Instant.now()),
                nextDeletionTime = getNextExecutionTime(job.deleteSchedule, Instant.now()),
            )
            updateMetadata()
        }
        return this
    }
}
