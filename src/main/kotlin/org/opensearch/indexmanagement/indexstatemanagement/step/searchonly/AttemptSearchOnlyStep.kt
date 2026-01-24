/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.searchonly

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.transport.RemoteTransportException

class AttemptSearchOnlyStep : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        val context = this.context
        if (context != null) {
            val indexName = context.metadata.index

            // Check if already in search-only mode (idempotency)
            val clusterState = context.clusterService.state()
            val indexMetadata = clusterState.metadata.index(indexName)
            if (indexMetadata != null) {
                val isSearchOnly = indexMetadata.settings.getAsBoolean(INDEX_BLOCKS_SEARCH_ONLY_SETTING, false)
                if (isSearchOnly) {
                    stepStatus = StepStatus.COMPLETED
                    info = mapOf("message" to getAlreadySearchOnlyMessage(indexName))
                    return this
                }
            }

            try {
                val response: AcknowledgedResponse =
                    context.client.admin().indices()
                        .suspendUntil { listener ->
                            prepareScaleSearchOnly(indexName, true).execute(listener)
                        }

                if (response.isAcknowledged) {
                    stepStatus = StepStatus.COMPLETED
                    info = mapOf("message" to getSuccessMessage(indexName))
                } else {
                    val message = getFailedMessage(indexName)
                    logger.warn(message)
                    stepStatus = StepStatus.FAILED
                    info = mapOf("message" to message)
                }
            } catch (e: RemoteTransportException) {
                val cause = ExceptionsHelper.unwrapCause(e)
                when (cause) {
                    is SnapshotInProgressException -> handleSnapshotException(indexName, cause)
                    is OpenSearchRejectedExecutionException -> handleRetryableException(indexName, cause)
                    else -> handleException(indexName, cause as Exception)
                }
            } catch (e: SnapshotInProgressException) {
                handleSnapshotException(indexName, e)
            } catch (e: OpenSearchRejectedExecutionException) {
                handleRetryableException(indexName, e)
            } catch (e: Exception) {
                if (isRetryableException(e)) {
                    handleRetryableException(indexName, e)
                } else {
                    handleException(indexName, e)
                }
            }
        }

        return this
    }

    private fun handleSnapshotException(indexName: String, e: SnapshotInProgressException) {
        val message = getSnapshotMessage(indexName)
        logger.warn(message, e)
        stepStatus = StepStatus.CONDITION_NOT_MET
        info = mapOf("message" to message)
    }

    private fun handleRetryableException(indexName: String, e: Exception) {
        val message = getRetryableMessage(indexName)
        logger.warn(message, e)
        stepStatus = StepStatus.CONDITION_NOT_MET
        info = mapOf("message" to message, "cause" to (e.message ?: "Unknown"))
    }

    private fun isRetryableException(e: Exception): Boolean {
        val message = e.message?.lowercase() ?: return false
        return message.contains("timeout") ||
            message.contains("timed out") ||
            message.contains("backpressure") ||
            message.contains("rejected")
    }

    private fun handleException(indexName: String, e: Exception) {
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData = currentMetadata.copy(
        stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
        transitionTo = null,
        info = info,
    )

    override fun isIdempotent(): Boolean = true

    companion object {
        const val name = "attempt_search_only"
        private const val INDEX_BLOCKS_SEARCH_ONLY_SETTING = "index.blocks.search_only"

        fun getFailedMessage(index: String) = "Failed to set index to search-only mode [index=$index]"

        fun getSuccessMessage(index: String) = "Successfully set index to search-only mode [index=$index]"

        fun getAlreadySearchOnlyMessage(index: String) = "Index is already in search-only mode [index=$index]"

        fun getSnapshotMessage(index: String) = "Index had snapshot in progress, retrying search_only action [index=$index]"

        fun getRetryableMessage(index: String) = "Temporary condition prevented search_only operation, retrying [index=$index]"
    }
}
