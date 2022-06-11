/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.client.Client
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementException
import org.opensearch.indexmanagement.snapshotmanagement.SnapshotManagementException.ExceptionKey
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMResult
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.smTransitions
import org.opensearch.indexmanagement.snapshotmanagement.indexMetadata
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.snapshotmanagement.smDocIdToPolicyName
import org.opensearch.indexmanagement.util.OpenForTesting
import java.time.Instant.now

@OpenForTesting
class SMStateMachine(
    val client: Client,
    val job: SMPolicy,
    var metadata: SMMetadata
) {

    val log: Logger = LogManager.getLogger("$javaClass [${smDocIdToPolicyName(job.id)}]")

    var currentState: SMState = metadata.currentState.also {
        log.info("Current state: [${metadata.currentState}].")
    }

    suspend fun next(transitions: Map<SMState, List<SMState>> = smTransitions) {
        try {
            do {
                val nextStates = transitions[currentState]
                if (nextStates == null) {
                    // Unlikely to reach unless the currentState field of metadata is tampered
                    log.error("No next states for current state [$currentState].")
                    return
                }

                lateinit var result: SMResult
                val prevState = currentState
                for (nextState in nextStates) {
                    currentState = nextState
                    log.info("sm dev: Start executing $currentState.")
                    result = nextState.instance.execute(this) as SMResult
                    when (result) {
                        is SMResult.Next -> {
                            log.info("State [$currentState] has finished.")
                            updateMetadata(result.metadataToSave.copy(currentState = currentState))
                            // break the nextStates loop, to avoid executing other lateral states
                            break
                        }
                        is SMResult.Stay -> {
                            log.info("State [$currentState] has not finished.")
                            updateMetadata(result.metadataToSave.copy(currentState = prevState))
                            // can still execute other lateral states if exists
                        }
                        is SMResult.Fail -> {
                            if (result.forceReset == true) {
                                val metadataToSave = SMMetadata.Builder(result.metadataToSave)
                                    .workflow(result.workflowType)
                                    .resetWorkflow()
                                    .build()
                                updateMetadata(metadataToSave)
                            } else {
                                updateMetadata(handleRetry(result, prevState).build())
                            }
                        }
                    }
                }

                if (result !is SMResult.Next) {
                    // Only Next result requires checking the continuous flag and
                    //  continue to execute next vertical state
                    break
                }
            } while (currentState.instance.continuous)
        } catch (ex: Exception) {
            if (ex is SnapshotManagementException &&
                ex.exKey == ExceptionKey.METADATA_INDEXING_FAILURE
            ) {
                // update metadata exception is special, we don't want to retry update metadata here
                return
            }
            log.error("Uncaught snapshot management runtime exception.", ex)
        }
    }

    private fun handleRetry(result: SMResult, prevState: SMState): SMMetadata.Builder {
        result as SMResult.Fail
        val metadata = result.metadataToSave
        val metadataBuilder = SMMetadata.Builder(metadata)
            .workflow(result.workflowType)
            .setCurrentState(prevState)
        val retry = when (result.workflowType) {
            WorkflowType.CREATION -> {
                metadata.creation.retry
            }
            WorkflowType.DELETION -> {
                metadata.deletion?.retry
            }
        }
        val retryCount: Int
        if (retry == null) {
            log.warn("Starting to retry state [$currentState], remaining count 3.")
            metadataBuilder.setRetry(3) // TODO SM 3 retry count could be customizable
        } else {
            retryCount = retry.count - 1
            if (retryCount > 0) {
                log.warn("Retrying state [$currentState], remaining count $retryCount.")
                metadataBuilder.setRetry(retryCount)
            } else {
                val errorMessage = "Retry count exhausted for state [$currentState], reset workflow ${result.workflowType}."
                log.warn(errorMessage)
                metadataBuilder.setLatestExecution(
                    status = SMMetadata.LatestExecution.Status.FAILED,
                    cause = errorMessage,
                    endTime = now()
                ).resetWorkflow()
            }
        }

        return metadataBuilder
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
    suspend fun updateMetadata(md: SMMetadata) {
        try {
            // TODO SM retry policy for update metadata call
            log.info("sm dev update metadata $md")
            val res = client.indexMetadata(md, job.id, metadataSeqNo, metadataPrimaryTerm)
            metadataSeqNo = res.seqNo
            metadataPrimaryTerm = res.primaryTerm
            metadata = md
        } catch (ex: Exception) {
            val smEx = SnapshotManagementException(ExceptionKey.METADATA_INDEXING_FAILURE, ex)
            log.error(smEx.message, ex)
            throw smEx
        }

        // TODO SM save a copy to history
    }

    /**
     * Handle the policy change before job running
     *
     * Currently, only handle schedule change in policy.
     */
    suspend fun handlePolicyChange(): SMStateMachine {
        if (job.seqNo > metadata.policySeqNo || job.primaryTerm > metadata.policyPrimaryTerm) {
            val now = now()
            val metadataToSave = SMMetadata.Builder(metadata)
                .setPolicyVersion(job.seqNo, job.primaryTerm)
                .setNextCreationTime(job.creation.schedule.getNextExecutionTime(now))

            job.deletion?.let {
                metadataToSave.setNextDeletionTime(job.deletion!!.schedule.getNextExecutionTime(now))
            }
            updateMetadata(metadataToSave.build())
        }
        return this
    }
}
