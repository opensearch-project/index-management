/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.restore

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS
import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript
import org.opensearch.snapshots.SnapshotException
import org.opensearch.snapshots.SnapshotState
import org.opensearch.transport.RemoteTransportException

class AttemptRestoreStep(private val action: ConvertIndexToRemoteAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var snapshotName: String? = null

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount", "LongMethod", "NestedBlockDepth")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val managedIndexMetadata = context.metadata
        val indexName = context.metadata.index
        val scriptService = context.scriptService
        val repository = action.repository
        val snapshot = action.snapshot

        try {
            val mutableInfo = mutableMapOf<String, Any>()
            val snapshotScript = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, snapshot, mapOf())
            val defaultSnapshotPattern = snapshot.ifBlank { indexName }
            val snapshotPattern = compileTemplate(snapshotScript, managedIndexMetadata, defaultSnapshotPattern, scriptService)

            // List snapshots matching the pattern
            val getSnapshotsRequest = GetSnapshotsRequest()
                .repository(repository)
                .snapshots(arrayOf("$snapshotPattern*"))
                .ignoreUnavailable(true)
                .verbose(true)

            val getSnapshotsResponse: GetSnapshotsResponse = context.client.admin().cluster().suspendUntil {
                getSnapshots(getSnapshotsRequest, it)
            }
            val snapshots = getSnapshotsResponse.snapshots
            if (snapshots.isNullOrEmpty()) {
                val message = getFailedMessage(indexName, "No snapshots found matching pattern [$snapshotPattern*]")
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
                return this
            }

            val successfulSnapshots = snapshots.filter { it.state() == SnapshotState.SUCCESS }

            if (successfulSnapshots.isEmpty()) {
                val message = getFailedMessage(
                    indexName,
                    "No successful snapshots found matching pattern [$snapshotPattern*]",
                )
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
                return this
            }

            // Select the latest snapshot
            val latestSnapshotInfo = successfulSnapshots.maxByOrNull { it.endTime() }!!
            logger.info("Restoring snapshot info: $latestSnapshotInfo")

            // Use the snapshot name from the selected SnapshotInfo
            snapshotName = latestSnapshotInfo.snapshotId().name

            // Calculate remote index name from rename pattern
            // renamePattern like "$1_remote" means replace $1 with the index name
            // So "myindex" with pattern "$1_remote" becomes "myindex_remote"
            val remoteIndexName = action.renamePattern.replace("\$1", indexName)

            // Check if remote index exists
            val remoteIndexExists = checkRemoteIndexExists(context, remoteIndexName)

            if (remoteIndexExists) {
                // Restore completed, mark as completed
                stepStatus = StepStatus.COMPLETED
                mutableInfo["message"] = getSuccessMessage(indexName)
            } else {
                performRestore(context, indexName, repository, snapshotName, mutableInfo)
            }
            info = mutableInfo.toMap()
        } catch (e: RemoteTransportException) {
            val cause = ExceptionsHelper.unwrapCause(e)
            if (cause is SnapshotException) {
                handleRestoreException(indexName, cause)
            } else {
                handleException(indexName, cause as Exception)
            }
        } catch (e: SnapshotException) {
            handleRestoreException(indexName, e)
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return this
    }

    private suspend fun checkRemoteIndexExists(context: StepContext, remoteIndexName: String): Boolean =
        try {
            val existsResponse: IndicesExistsResponse = context.client.admin().indices().suspendUntil {
                exists(IndicesExistsRequest(remoteIndexName), it)
            }
            existsResponse.isExists
        } catch (e: Exception) {
            // Index doesn't exist yet
            false
        }

    private suspend fun performRestore(
        context: StepContext,
        indexName: String,
        repository: String,
        snapshotName: String?,
        mutableInfo: MutableMap<String, Any>,
    ) {
        // Calculate remote index name from rename pattern
        val remoteIndexName = action.renamePattern.replace("\$1", indexName)
        // Proceed with the restore operation
        val restoreSnapshotRequest = RestoreSnapshotRequest(repository, snapshotName)
            .indices(indexName)
            .storageType(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT)
            .renamePattern("^(.*)\$")
            .renameReplacement(action.renamePattern)
            .waitForCompletion(false)
            .includeAliases(action.includeAliases)
            .ignoreIndexSettings(action.ignoreIndexSettings)

        // Set number_of_replicas (defaults to 0 if not specified)
        val indexSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_REPLICAS, action.numberOfReplicas)
            .build()
        restoreSnapshotRequest.indexSettings(indexSettings)

        val response: RestoreSnapshotResponse = context.client.admin().cluster().suspendUntil {
            restoreSnapshot(restoreSnapshotRequest, it)
        }

        when (response.status()) {
            RestStatus.ACCEPTED, RestStatus.OK -> {
                // Only delete original index if explicitly enabled
                if (action.deleteOriginalIndex) {
                    deleteOriginalIndex(context, indexName, mutableInfo)
                }
                // Mark as waiting for completion
                stepStatus = StepStatus.CONDITION_NOT_MET
                mutableInfo["message"] = "Waiting for remote index [$remoteIndexName] to be created"
                logger.info("Restore accepted for snapshot [$snapshotName], waiting for remote index [$remoteIndexName] to be created")
            }

            else -> {
                val message = getFailedMessage(indexName, "Unexpected response status: ${response.status()}")
                logger.warn("$message - $response")
                stepStatus = StepStatus.FAILED
                mutableInfo["message"] = message
                mutableInfo["cause"] = response.toString()
            }
        }
    }

    private suspend fun deleteOriginalIndex(
        context: StepContext,
        indexName: String,
        mutableInfo: MutableMap<String, Any>,
    ) {
        // Restore accepted, delete original index
        try {
            val deleteResponse: AcknowledgedResponse = context.client.admin().indices().suspendUntil {
                delete(DeleteIndexRequest(indexName), it)
            }
            if (deleteResponse.isAcknowledged) {
                logger.info("Successfully deleted original index [$indexName] after restore was accepted")
                mutableInfo["deleted_original_index"] = true
            } else {
                logger.warn("Delete request for original index [$indexName] was not acknowledged")
                mutableInfo["deleted_original_index"] = false
            }
        } catch (e: Exception) {
            logger.warn("Failed to delete original index [$indexName] after restore was accepted: ${e.message}", e)
            mutableInfo["deleted_original_index"] = false
            mutableInfo["delete_error"] = e.message ?: "Unknown error"
        }
    }

    private fun compileTemplate(
        template: Script,
        managedIndexMetaData: ManagedIndexMetaData,
        defaultValue: String,
        scriptService: ScriptService,
    ): String {
        val contextMap =
            managedIndexMetaData.convertToMap().filterKeys { key ->
                key in validTopContextFields
            }
        val compiledValue =
            scriptService.compile(template, TemplateScript.CONTEXT)
                .newInstance(template.params + mapOf("ctx" to contextMap))
                .execute()
        return compiledValue.ifBlank { defaultValue }
    }

    private fun handleRestoreException(indexName: String, e: SnapshotException) {
        val message = getFailedRestoreMessage(indexName)
        logger.debug(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf<String, Any>("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    private fun handleException(indexName: String, e: Exception) {
        val message = getFailedMessage(indexName, e.message ?: "Unknown error")
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf<String, Any>("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetadata.actionMetaData
        return currentMetadata.copy(
            actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(snapshotName = snapshotName)),
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info,
        )
    }

    override fun isIdempotent(): Boolean = false

    companion object {
        val validTopContextFields = setOf("index", "indexUuid")
        const val name = "attempt_restore"
        fun getFailedMessage(index: String, cause: String) = "Failed to start restore for [index=$index], cause: $cause"
        fun getFailedRestoreMessage(index: String) = "Failed to start restore due to concurrent restore or snapshot in progress [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully started restore for [index=$index]"
    }
}
