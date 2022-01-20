/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.snapshot

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.common.regex.Regex
import org.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.SNAPSHOT_DENY_LIST
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript
import org.opensearch.snapshots.ConcurrentSnapshotExecutionException
import org.opensearch.transport.RemoteTransportException
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale

class AttemptSnapshotStep(private val action: SnapshotAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var snapshotName: String? = null

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount", "LongMethod")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val managedIndexMetadata = context.metadata
        val scriptService = context.scriptService
        val denyList: List<String> = context.clusterService.clusterSettings.get(SNAPSHOT_DENY_LIST)
        val repository = action.repository
        val snapshot = action.snapshot
        try {
            val mutableInfo = mutableMapOf<String, String>()

            if (isDenied(denyList, repository)) {
                stepStatus = StepStatus.FAILED
                mutableInfo["message"] = getBlockedMessage(denyList, repository, indexName)
                info = mutableInfo.toMap()
                return this
            }
            val snapshotNameSuffix = "-".plus(
                LocalDateTime.now(ZoneId.of("UTC"))
                    .format(DateTimeFormatter.ofPattern("uuuu.MM.dd-HH:mm:ss.SSS", Locale.ROOT))
            )

            val snapshotScript = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, snapshot, mapOf())
            // If user intentionally set the snapshot name empty then we are going to honor it
            val defaultSnapshotName = if (snapshot.isBlank()) snapshot else indexName
            snapshotName = compileTemplate(snapshotScript, managedIndexMetadata, defaultSnapshotName, scriptService).plus(snapshotNameSuffix)

            val createSnapshotRequest = CreateSnapshotRequest()
                .userMetadata(mapOf("snapshot_created" to "Open Distro for Elasticsearch Index Management"))
                .indices(indexName)
                .snapshot(snapshotName)
                .repository(repository)
                .waitForCompletion(false)

            val response: CreateSnapshotResponse = context.client.admin().cluster().suspendUntil { createSnapshot(createSnapshotRequest, it) }
            when (response.status()) {
                RestStatus.ACCEPTED -> {
                    stepStatus = StepStatus.COMPLETED
                    mutableInfo["message"] = getSuccessMessage(indexName)
                }
                RestStatus.OK -> {
                    stepStatus = StepStatus.COMPLETED
                    mutableInfo["message"] = getSuccessMessage(indexName)
                }
                else -> {
                    val message = getFailedMessage(indexName)
                    logger.warn("$message - $response")
                    stepStatus = StepStatus.FAILED
                    mutableInfo["message"] = getFailedMessage(indexName)
                    mutableInfo["cause"] = response.toString()
                }
            }
            info = mutableInfo.toMap()
        } catch (e: RemoteTransportException) {
            val cause = ExceptionsHelper.unwrapCause(e)
            if (cause is ConcurrentSnapshotExecutionException) {
                handleSnapshotException(indexName, cause)
            } else {
                handleException(indexName, cause as Exception)
            }
        } catch (e: ConcurrentSnapshotExecutionException) {
            handleSnapshotException(indexName, e)
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return this
    }

    private fun isDenied(denyList: List<String>, repoName: String): Boolean {
        val predicate = { pattern: String -> Regex.simpleMatch(pattern, repoName) }
        return denyList.stream().anyMatch(predicate)
    }

    private fun handleSnapshotException(indexName: String, e: ConcurrentSnapshotExecutionException) {
        val message = getFailedConcurrentSnapshotMessage(indexName)
        logger.debug(message, e)
        stepStatus = StepStatus.CONDITION_NOT_MET
        info = mapOf("message" to message)
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

    private fun compileTemplate(
        template: Script,
        managedIndexMetaData: ManagedIndexMetaData,
        defaultValue: String,
        scriptService: ScriptService
    ): String {
        val contextMap = managedIndexMetaData.convertToMap().filterKeys { key ->
            key in validTopContextFields
        }
        val compiledValue = scriptService.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to contextMap))
            .execute()
        return if (compiledValue.isBlank()) defaultValue else compiledValue
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetadata.actionMetaData
        return currentMetadata.copy(
            actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(snapshotName = snapshotName)),
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent(): Boolean = false

    companion object {
        val validTopContextFields = setOf("index", "indexUuid")
        const val name = "attempt_snapshot"
        fun getBlockedMessage(denyList: List<String>, repoName: String, index: String) =
            "Snapshot repository [$repoName] is blocked in $denyList [index=$index]"
        fun getFailedMessage(index: String) = "Failed to create snapshot [index=$index]"
        fun getFailedConcurrentSnapshotMessage(index: String) = "Concurrent snapshot in progress, retrying next execution [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully started snapshot [index=$index]"
    }
}
