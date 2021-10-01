/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.snapshot

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.regex.Regex
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.SnapshotActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.SNAPSHOT_DENY_LIST
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
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

class AttemptSnapshotStep(
    val clusterService: ClusterService,
    val scriptService: ScriptService,
    val client: Client,
    val config: SnapshotActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var snapshotName: String? = null
    private var denyList: List<String> = clusterService.clusterSettings.get(SNAPSHOT_DENY_LIST)

    override fun isIdempotent() = false

    @Suppress("TooGenericExceptionCaught", "ComplexMethod")
    override suspend fun execute(): AttemptSnapshotStep {
        try {
            val mutableInfo = mutableMapOf<String, String>()

            if (isDenied(denyList, config.repository)) {
                stepStatus = StepStatus.FAILED
                mutableInfo["message"] = getBlockedMessage(denyList, config.repository, indexName)
                info = mutableInfo.toMap()
                return this
            }
            val snapshotNameSuffix = "-".plus(
                LocalDateTime.now(ZoneId.of("UTC"))
                    .format(DateTimeFormatter.ofPattern("uuuu.MM.dd-HH:mm:ss.SSS", Locale.ROOT))
            )

            val snapshotScript = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, config.snapshot, mapOf())
            snapshotName = compileTemplate(snapshotScript, managedIndexMetaData, indexName).plus(snapshotNameSuffix)

            val createSnapshotRequest = CreateSnapshotRequest()
                .userMetadata(mapOf("snapshot_created" to "Open Distro for Elasticsearch Index Management"))
                .indices(indexName)
                .snapshot(snapshotName)
                .repository(config.repository)
                .waitForCompletion(false)

            val response: CreateSnapshotResponse = client.admin().cluster().suspendUntil { createSnapshot(createSnapshotRequest, it) }
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
                handleSnapshotException(cause)
            } else {
                handleException(cause as Exception)
            }
        } catch (e: ConcurrentSnapshotExecutionException) {
            handleSnapshotException(e)
        } catch (e: Exception) {
            handleException(e)
        }

        return this
    }

    private fun isDenied(denyList: List<String>, repoName: String): Boolean {
        val predicate = { pattern: String -> Regex.simpleMatch(pattern, repoName) }
        return denyList.stream().anyMatch(predicate)
    }

    private fun handleSnapshotException(e: ConcurrentSnapshotExecutionException) {
        val message = getFailedConcurrentSnapshotMessage(indexName)
        logger.debug(message, e)
        stepStatus = StepStatus.CONDITION_NOT_MET
        info = mapOf("message" to message)
    }

    private fun handleException(e: Exception) {
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    private fun compileTemplate(template: Script, managedIndexMetaData: ManagedIndexMetaData, defaultValue: String): String {
        val contextMap = managedIndexMetaData.convertToMap().filterKeys { key ->
            key in validTopContextFields
        }
        val compiledValue = scriptService.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to contextMap))
            .execute()
        return if (compiledValue.isBlank()) defaultValue else compiledValue
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetaData.actionMetaData
        return currentMetaData.copy(
            actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(snapshotName = snapshotName)),
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

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
