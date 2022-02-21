/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.getNextExecutionTime
import org.opensearch.indexmanagement.snapshotmanagement.indexMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SM
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.rest.RestStatus
import java.time.Instant

/**
 * Context for state machine execution
 */
class SMStateMachine(
    val client: Client,
    val job: SM,
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
            // loop for vertical level
            do {
                val nextStates = transitions[currentState]
                if (nextStates == null) {
                    log.info("Next state is null")
                    return
                }
                log.info("Next states: $nextStates")
                var executionEndState = false
                // loop for same level
                for (nextState in nextStates) {
                    currentState = nextState
                    log.info("Going to execute $currentState")
                    executionEndState = nextState.instance.execute(this)
                    if (executionEndState) {
                        log.info("[$currentState] execution satisfied, move to this state.")
                        updateMetadata()
                        break
                    } else {
                        log.info("[$currentState] execution not satisfied, stay in previous state.")
                    }
                }
                if (!executionEndState) {
                    log.info("$nextStates execution not satisfied, wait for next job run.")
                    break
                }
                log.info("[$currentState] continuous flag: ${currentState.instance.continuous}")
            } while (currentState.instance.continuous)
        } catch (e: Exception) {
            // TODO state execution failed with some exception, check timeout
            //  if no timeout start time, add it, update exception info
            //  if pass the timeout period ..., update exception info
            //  if not, wait for next job run, update exception info
            log.error("Failed executing state $currentState", e)
        }
    }

    /**
     * if any failure during update metadata, the best we can do is to log out error
     * and state machine can only continue execution when it can update metadata
     */
    suspend fun updateMetadata(md: SMMetadata? = metadataToSave) {
        if (md == null) {
            throw NullPointerException("Null metadataToSave")
        }

        val res = client.indexMetadata(md, job.policyName, metadataSeqNo, metadataPrimaryTerm)
        if (res.status() != RestStatus.OK) {
            log.info("Update metadata call status code ${res.status()}")
            // TODO retry and log out, throw exception is not useful
        }

        metadataSeqNo = res.seqNo
        metadataPrimaryTerm = res.primaryTerm
        metadata = md
        metadataToSave = null // force any metadataToSave to base on metadata
    }

    suspend fun handlePolicyChange() {
        if (job.seqNo != metadata.policySeqNo || job.primaryTerm != metadata.policyPrimaryTerm) {
            metadataToSave = metadata.copy(
                policySeqNo = job.seqNo,
                policyPrimaryTerm = job.primaryTerm,
                nextCreationTime = getNextExecutionTime(job.createSchedule, Instant.now()),
                nextDeletionTime = getNextExecutionTime(job.deleteSchedule, Instant.now()),
            )
            updateMetadata()
        }
    }
}
