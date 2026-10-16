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
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
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

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "CyclomaticComplexMethod", "ReturnCount", "LongMethod", "NestedBlockDepth")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val managedIndexMetadata = context.metadata
        val indexName = context.metadata.index
        val scriptService = context.scriptService
        val repository = action.repository
        val snapshot = action.snapshot

        if (action.numberOfReplicas != null && action.numberOfReplicas < 0) {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailedMessage(indexName, "number_of_replicas must be non-negative"))
            return this
        }

        if (action.renamePattern.isBlank() || !action.renamePattern.contains("\$1")) {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailedMessage(indexName, "rename_pattern must be non-empty and contain '\$1'"))
            return this
        }

        try {
            val mutableInfo = mutableMapOf<String, Any>()
            val snapshotScript = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, snapshot, mapOf())
            val defaultSnapshotPattern = snapshot.ifBlank { indexName }
            val snapshotPattern = compileTemplate(snapshotScript, managedIndexMetadata, defaultSnapshotPattern, scriptService)

            val remoteIndexName = action.renamePattern.replace("\$1", indexName)

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

            val remoteIndexExists = checkRemoteIndexExists(context, remoteIndexName)

            if (remoteIndexExists) {
                if (action.deleteOriginalIndex) {
                    if (action.addOriginalNameAsAlias) {
                        // Atomic: delete original index + add alias in one request
                        val success = deleteOriginalAndAddAlias(context, indexName, remoteIndexName, mutableInfo)
                        if (!success) {
                            stepStatus = StepStatus.FAILED
                            mutableInfo["message"] = getFailedMessage(indexName, "Failed to delete original index and add alias")
                            info = mutableInfo.toMap()
                            return this
                        }
                    } else {
                        val deleted = deleteOriginalIndex(context, indexName, mutableInfo)
                        if (!deleted) {
                            stepStatus = StepStatus.FAILED
                            mutableInfo["message"] = getFailedMessage(indexName, "Failed to delete original index")
                            info = mutableInfo.toMap()
                            return this
                        }
                    }
                }
                stepStatus = StepStatus.COMPLETED
                mutableInfo["message"] = getSuccessMessage(indexName)
            } else {
                performRestore(context, indexName, remoteIndexName, repository, snapshotName, mutableInfo)
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

    @Suppress("ReturnCount")
    private fun checkRemoteIndexExists(context: StepContext, remoteIndexName: String): Boolean {
        val metadata = context.clusterService.state().metadata()
        if (!metadata.hasIndex(remoteIndexName)) return false
        val indexMetadata = metadata.index(remoteIndexName) ?: return false
        val storeType = indexMetadata.settings.get("index.store.type") ?: return false
        return storeType == "remote_snapshot"
    }

    private suspend fun performRestore(
        context: StepContext,
        indexName: String,
        remoteIndexName: String,
        repository: String,
        snapshotName: String?,
        mutableInfo: MutableMap<String, Any>,
    ) {
        val restoreSnapshotRequest = RestoreSnapshotRequest(repository, snapshotName)
            .indices(indexName)
            .storageType(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT)
            .renamePattern("^(.*)\$")
            .renameReplacement(action.renamePattern)
            .waitForCompletion(false)
            .includeAliases(action.includeAliases)

        if (action.ignoreIndexSettings.isNotBlank()) {
            val settingsList = action.ignoreIndexSettings.split(",").map { it.trim() }.filter { it.isNotEmpty() }
            restoreSnapshotRequest.ignoreIndexSettings(*settingsList.toTypedArray())
        }

        if (action.numberOfReplicas != null) {
            val indexSettings = Settings.builder()
                .put(SETTING_NUMBER_OF_REPLICAS, action.numberOfReplicas)
                .build()
            restoreSnapshotRequest.indexSettings(indexSettings)
        }

        val response: RestoreSnapshotResponse = context.client.admin().cluster().suspendUntil {
            restoreSnapshot(restoreSnapshotRequest, it)
        }

        when (response.status()) {
            RestStatus.ACCEPTED, RestStatus.OK -> {
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

    @Suppress("ReturnCount")
    private suspend fun deleteOriginalIndex(
        context: StepContext,
        indexName: String,
        mutableInfo: MutableMap<String, Any>,
    ): Boolean {
        if (!context.clusterService.state().metadata().hasIndex(indexName)) {
            logger.info("Original index [$indexName] already deleted, nothing to do")
            return true
        }
        try {
            val deleteResponse: AcknowledgedResponse = context.client.admin().indices().suspendUntil {
                delete(DeleteIndexRequest(indexName), it)
            }
            if (deleteResponse.isAcknowledged) {
                logger.info("Successfully deleted original index [$indexName] after remote index was confirmed")
                return true
            } else {
                logger.warn("Delete request for original index [$indexName] was not acknowledged")
                mutableInfo["delete_error"] = "Delete request was not acknowledged"
                return false
            }
        } catch (e: Exception) {
            logger.warn("Failed to delete original index [$indexName]: ${e.message}", e)
            mutableInfo["delete_error"] = e.message ?: "Unknown error"
            return false
        }
    }

    @Suppress("ReturnCount")
    private suspend fun deleteOriginalAndAddAlias(
        context: StepContext,
        originalIndexName: String,
        remoteIndexName: String,
        mutableInfo: MutableMap<String, Any>,
    ): Boolean {
        // Idempotency: if original is already gone, check if alias exists
        if (!context.clusterService.state().metadata().hasIndex(originalIndexName)) {
            logger.info("Original index [$originalIndexName] already deleted")
            val metadata = context.clusterService.state().metadata()
            val indexMetadata = metadata.index(remoteIndexName)
            if (indexMetadata != null && indexMetadata.aliases.containsKey(originalIndexName)) {
                logger.info("Alias [$originalIndexName] already exists on remote index [$remoteIndexName], nothing to do")
                return true
            }
            // Original gone but alias missing - just add alias
            return addAliasOnly(context, remoteIndexName, originalIndexName, mutableInfo)
        }
        try {
            val aliasRequest = IndicesAliasesRequest()
            aliasRequest.addAliasAction(
                AliasActions(AliasActions.Type.REMOVE_INDEX).index(originalIndexName),
            )
            aliasRequest.addAliasAction(
                AliasActions(AliasActions.Type.ADD)
                    .index(remoteIndexName)
                    .alias(originalIndexName),
            )
            val response: AcknowledgedResponse = context.client.admin().indices().suspendUntil {
                aliases(aliasRequest, it)
            }
            if (response.isAcknowledged) {
                logger.info("Successfully deleted [$originalIndexName] and added it as alias to [$remoteIndexName]")
                return true
            } else {
                logger.warn("Atomic delete+alias request for [$originalIndexName] was not acknowledged")
                mutableInfo["alias_error"] = "Atomic delete+alias request was not acknowledged"
                return false
            }
        } catch (e: Exception) {
            logger.warn("Failed atomic delete+alias for [$originalIndexName] to [$remoteIndexName]: ${e.message}", e)
            mutableInfo["alias_error"] = e.message ?: "Unknown error"
            return false
        }
    }

    @Suppress("ReturnCount")
    private suspend fun addAliasOnly(
        context: StepContext,
        remoteIndexName: String,
        originalIndexName: String,
        mutableInfo: MutableMap<String, Any>,
    ): Boolean {
        try {
            val aliasRequest = IndicesAliasesRequest()
            aliasRequest.addAliasAction(
                AliasActions(AliasActions.Type.ADD)
                    .index(remoteIndexName)
                    .alias(originalIndexName),
            )
            val response: AcknowledgedResponse = context.client.admin().indices().suspendUntil {
                aliases(aliasRequest, it)
            }
            if (response.isAcknowledged) {
                logger.info("Successfully added alias [$originalIndexName] to remote index [$remoteIndexName]")
                return true
            } else {
                logger.warn("Adding alias [$originalIndexName] to [$remoteIndexName] was not acknowledged")
                mutableInfo["alias_error"] = "Alias request was not acknowledged"
                return false
            }
        } catch (e: Exception) {
            logger.warn("Failed to add alias [$originalIndexName] to [$remoteIndexName]: ${e.message}", e)
            mutableInfo["alias_error"] = e.message ?: "Unknown error"
            return false
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
