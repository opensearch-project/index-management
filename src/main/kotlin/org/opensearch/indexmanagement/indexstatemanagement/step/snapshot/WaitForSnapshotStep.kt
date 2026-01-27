/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.snapshot

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import org.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.snapshots.SnapshotInfo
import org.opensearch.snapshots.SnapshotState
import org.opensearch.transport.RemoteTransportException

class WaitForSnapshotStep(private val action: SnapshotAction) : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("CyclomaticComplexMethod", "ReturnCount")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val managedIndexMetadata = context.metadata
        val repository = action.repository

        try {
            val snapshotName = getSnapshotName(managedIndexMetadata, indexName) ?: return this
            val newRequest = GetSnapshotsRequest()
                .snapshots(arrayOf(snapshotName))
                .repository(repository)
            val response: GetSnapshotsResponse = context.client.admin().cluster().suspendUntil { getSnapshots(newRequest, it) }
            val status: SnapshotInfo? =
                response
                    .snapshots
                    .find { snapshotInfo ->
                        snapshotInfo.snapshotId().name == snapshotName
                    }
            if (status != null) {
                when (status.state()) {
                    SnapshotState.IN_PROGRESS -> {
                        stepStatus = StepStatus.CONDITION_NOT_MET
                        info = mapOf("message" to getSnapshotInProgressMessage(indexName), "state" to status.state().toString())
                    }

                    SnapshotState.SUCCESS -> {
                        stepStatus = StepStatus.COMPLETED
                        info = mapOf("message" to getSuccessMessage(indexName), "state" to status.state().toString())
                    }

                    else -> { // State.FAILED, State.ABORTED
                        val message = getFailedExistsMessage(indexName)
                        logger.warn(message)
                        stepStatus = StepStatus.FAILED
                        info = mapOf("message" to message, "state" to status.state().toString())
                    }
                }
            } else {
                val message = getFailedExistsMessage(indexName)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
            }
        } catch (e: RemoteTransportException) {
            handleException(indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return this
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

    private fun getSnapshotName(managedIndexMetadata: ManagedIndexMetaData, indexName: String): String? {
        val actionProperties = managedIndexMetadata.actionMetaData?.actionProperties

        if (actionProperties?.snapshotName == null) {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailedActionPropertiesMessage(indexName, actionProperties))
            return null
        }

        return actionProperties.snapshotName
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData = currentMetadata.copy(
        stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
        transitionTo = null,
        info = info,
    )

    override fun isIdempotent(): Boolean = true

    companion object {
        const val name = "wait_for_snapshot"

        fun getFailedMessage(index: String) = "Failed to get status of snapshot [index=$index]"

        fun getFailedExistsMessage(index: String) = "Snapshot doesn't exist [index=$index]"

        fun getFailedActionPropertiesMessage(index: String, actionProperties: ActionProperties?) =
            "Unable to retrieve [${ActionProperties.Properties.SNAPSHOT_NAME.key}] from ActionProperties=$actionProperties [index=$index]"

        fun getSuccessMessage(index: String) = "Successfully created snapshot [index=$index]"

        fun getSnapshotInProgressMessage(index: String) = "Snapshot currently in progress [index=$index]"
    }
}
