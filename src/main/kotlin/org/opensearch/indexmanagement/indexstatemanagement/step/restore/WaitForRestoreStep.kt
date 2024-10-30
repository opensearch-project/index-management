/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.restore

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException

class WaitForRestoreStep : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index

        try {
            val clusterState = context.clusterService.state()
            val restoreInProgress = clusterState.custom<RestoreInProgress>(RestoreInProgress.TYPE)

            val restoreOngoing = restoreInProgress?.let { rip ->
                rip.any { entry ->
                    entry.indices().contains(indexName)
                }
            } ?: false

            if (restoreOngoing) {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getPendingMessage(indexName))
            } else {
                // Restore is complete
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getSuccessMessage(indexName))
            }
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return this
    }

    private fun handleException(indexName: String, e: Exception) {
        val message = getFailedMessage(indexName, e.message ?: "Unknown error")
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val cause = (e as? RemoteTransportException)?.cause

        val errorMessage = cause?.message ?: e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info,
        )
    }

    override fun isIdempotent(): Boolean = true

    companion object {
        const val name = "wait_for_restore"
        fun getFailedMessage(index: String, cause: String) = "Failed to check restore status for [index=$index], cause: $cause"
        fun getPendingMessage(index: String) = "Restore not complete for [index=$index], retrying..."
        fun getSuccessMessage(index: String) = "Restore complete for [index=$index]"
    }
}
