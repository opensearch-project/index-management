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
import org.opensearch.indexmanagement.snapshotmanagement.preFixTimeStamp
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState
import org.opensearch.indexmanagement.snapshotmanagement.engine.states.WorkflowType
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

                var result: SMResult = SMResult.Stay()
                for (nextState in nextStates) {
                    val prevState = currentState
                    currentState = nextState
                    log.info("sm dev: Start executing $currentState.")
                    result = nextState.instance.execute(this) as SMResult
                    when (result) {
                        is SMResult.Next -> {
                            log.info("State [$currentState]'s execution finished. Will execute the next.")
                            updateMetadata(
                                result.metadataToSave
                                    .copy(currentState = currentState)
                            )
                            // break the nextStates loop, to avoid executing other lateral states
                            break
                        }
                        is SMResult.Stay -> {
                            log.info("State [$currentState]'s execution not finished. Will stay.")
                            val metadataToSave = result.metadataToSave
                            metadataToSave?.let {
                                updateMetadata(
                                    metadataToSave
                                        .copy(currentState = prevState)
                                )
                            }
                            // can still execute other lateral states if exists
                        }
                        is SMResult.Failure -> {
                            val ex = result.ex
                            val userMessage = preFixTimeStamp(SnapshotManagementException(ex).message)
                            val info = metadata.info.upsert("exception" to userMessage)
                            val metadataToSave = SMMetadata.Builder(metadata)
                                .reset(result.workflowType)
                            if (result.notifiable) {
                                log.error("Caught exception while executing state [$currentState]. Reset the workflow ${result.workflowType}.", ex)
                                metadataToSave.info(info)
                                // TODO error notification
                            }

                            updateMetadata(metadataToSave.build())
                        }
                        is SMResult.Retry -> {
                            val metadataToSave = SMMetadata.Builder(metadata)
                                .currentState(prevState)
                            val retry = when (result.workflowType) {
                                WorkflowType.CREATION -> {
                                    metadata.creation.retry
                                }
                                WorkflowType.DELETION -> {
                                    metadata.deletion.retry
                                }
                            }
                            val retryCount: Int
                            if (retry == null) {
                                metadataToSave.setRetry(result.workflowType, 3)
                            } else {
                                retryCount = retry.count - 1
                                if (retryCount <= 0) {
                                    metadataToSave.reset(result.workflowType)
                                } else {
                                    metadataToSave.setRetry(result.workflowType, retryCount)
                                }
                            }

                            updateMetadata(metadataToSave.build())
                        }
                        is SMResult.TimeLimitExceed -> {
                            log.warn("${result.workflowType} has exceeded the time limit.")
                            val metadataToSave = SMMetadata.Builder(metadata)
                                .reset(result.workflowType)
                                .build()
                            updateMetadata(metadataToSave)
                        }
                    }
                }

                if (result !is SMResult.Next) {
                    // Only Next result requires checking the continuous flag and may continue execute
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
        try {
            // TODO SM retry policy for update metadata call
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
