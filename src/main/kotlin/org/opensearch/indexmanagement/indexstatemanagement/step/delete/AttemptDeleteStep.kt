/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.delete

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.transport.RemoteTransportException

class AttemptDeleteStep(private val action: DeleteAction) : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("CyclomaticComplexMethod")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index

        var snapshotRepository: String? = null
        var snapshotName: String? = null

        if (action.deleteSnapshot) {
            try {
                val settingsResponse = getIndexSettings(indexName)
                val settings = settingsResponse.indexToSettings[indexName]
                snapshotRepository = settings?.get(SEARCHABLE_SNAPSHOT_REPOSITORY_SETTING)
                snapshotName = settings?.get(SEARCHABLE_SNAPSHOT_NAME_SETTING)

                if (snapshotRepository != null && snapshotName != null) {
                    val otherIndicesUsingSameSnapshot = findOtherIndicesUsingSameSnapshot(
                        indexName,
                        snapshotRepository,
                        snapshotName,
                    )
                    if (otherIndicesUsingSameSnapshot.isNotEmpty()) {
                        logger.info(
                            "Snapshot [$snapshotRepository:$snapshotName] is used by other indices $otherIndicesUsingSameSnapshot, " +
                                "will skip snapshot deletion [index=$indexName]",
                        )
                        // Clear snapshot info to skip deletion later
                        snapshotRepository = null
                        snapshotName = null
                    }
                }
            } catch (e: Exception) {
                handleException(indexName, e, "Failed to get index settings")
            }
        }

        try {
            val response: AcknowledgedResponse =
                context.client.admin().indices()
                    .suspendUntil { delete(DeleteIndexRequest(indexName), it) }

            if (response.isAcknowledged) {
                if (action.deleteSnapshot && snapshotRepository != null && snapshotName != null) {
                    deleteSnapshot(indexName, snapshotRepository, snapshotName)
                } else {
                    stepStatus = StepStatus.COMPLETED
                    info = mapOf("message" to getSuccessMessage(indexName))
                }
            } else {
                val message = getFailedMessage(indexName)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
            }
        } catch (e: RemoteTransportException) {
            val cause = ExceptionsHelper.unwrapCause(e)
            if (cause is SnapshotInProgressException) {
                handleSnapshotInProgressException(indexName, cause)
            } else {
                handleException(indexName, cause as Exception)
            }
        } catch (e: SnapshotInProgressException) {
            handleSnapshotInProgressException(indexName, e)
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return this
    }

    private suspend fun getIndexSettings(indexName: String): GetSettingsResponse {
        val context = this.context!!
        return context.client.admin().indices()
            .suspendUntil { getSettings(GetSettingsRequest().indices(indexName), it) }
    }

    private suspend fun findOtherIndicesUsingSameSnapshot(
        currentIndex: String,
        repository: String,
        snapshotName: String,
    ): List<String> {
        val context = this.context!!
        val allSettingsResponse: GetSettingsResponse =
            context.client.admin().indices()
                .suspendUntil { getSettings(GetSettingsRequest().indices("*"), it) }

        return allSettingsResponse.indexToSettings
            .filter { (indexName, settings) ->
                indexName != currentIndex &&
                    settings.get(SEARCHABLE_SNAPSHOT_REPOSITORY_SETTING) == repository &&
                    settings.get(SEARCHABLE_SNAPSHOT_NAME_SETTING) == snapshotName
            }
            .keys
            .toList()
    }

    private suspend fun deleteSnapshot(indexName: String, repository: String, snapshotName: String) {
        val context = this.context!!
        try {
            val deleteSnapshotResponse: AcknowledgedResponse =
                context.client.admin().cluster()
                    .suspendUntil { deleteSnapshot(DeleteSnapshotRequest(repository, snapshotName), it) }

            if (deleteSnapshotResponse.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getSuccessWithSnapshotMessage(indexName, repository, snapshotName))
            } else {
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to getSnapshotDeleteFailedMessage(indexName, repository, snapshotName))
            }
        } catch (e: Exception) {
            handleException(indexName, e, "Index deleted but failed to delete snapshot [$repository:$snapshotName]")
        }
    }

    private fun handleSnapshotInProgressException(indexName: String, e: SnapshotInProgressException) {
        val message = getSnapshotInProgressMessage(indexName)
        logger.warn(message, e)
        stepStatus = StepStatus.CONDITION_NOT_MET
        info = mapOf("message" to message)
    }

    private fun handleException(indexName: String, e: Exception, customMessage: String? = null) {
        val message = customMessage ?: getFailedMessage(indexName)
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

    override fun isIdempotent() = true

    companion object {
        const val name = "attempt_delete"
        const val SEARCHABLE_SNAPSHOT_REPOSITORY_SETTING = "index.searchable_snapshot.repository"
        const val SEARCHABLE_SNAPSHOT_NAME_SETTING = "index.searchable_snapshot.snapshot_id.name"

        fun getFailedMessage(indexName: String) = "Failed to delete index [index=$indexName]"

        fun getSuccessMessage(indexName: String) = "Successfully deleted index [index=$indexName]"

        fun getSuccessWithSnapshotMessage(indexName: String, repository: String, snapshotName: String) =
            "Successfully deleted index [index=$indexName] and snapshot [$repository:$snapshotName]"

        fun getSnapshotInProgressMessage(indexName: String) =
            "Index had snapshot in progress, retrying deletion [index=$indexName]"

        fun getSnapshotDeleteFailedMessage(indexName: String, repository: String, snapshotName: String) =
            "Index deleted but failed to delete snapshot [index=$indexName, snapshot=$repository:$snapshotName]"
    }
}
