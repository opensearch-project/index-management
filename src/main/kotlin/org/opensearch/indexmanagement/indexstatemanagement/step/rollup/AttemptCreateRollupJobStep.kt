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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.indexmanagement.indexstatemanagement.step.rollup

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupAction
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupRequest
import org.opensearch.indexmanagement.rollup.action.index.IndexRollupResponse
import org.opensearch.indexmanagement.rollup.action.start.StartRollupAction
import org.opensearch.indexmanagement.rollup.action.start.StartRollupRequest
import org.opensearch.indexmanagement.rollup.model.ISMRollup
import org.opensearch.transport.RemoteTransportException
import java.lang.Exception

class AttemptCreateRollupJobStep(
    val clusterService: ClusterService,
    val client: Client,
    val ismRollup: ISMRollup,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var rollupId: String? = null
    private var previousRunRollupId: String? = null
    private var hasPreviousRollupAttemptFailed: Boolean? = null

    override fun isIdempotent() = true

    override suspend fun execute(): Step {
        previousRunRollupId = managedIndexMetaData.actionMetaData?.actionProperties?.rollupId
        hasPreviousRollupAttemptFailed = managedIndexMetaData.actionMetaData?.actionProperties?.hasRollupFailed

        // Creating a rollup job
        val rollup = ismRollup.toRollup(indexName)
        rollupId = rollup.id
        logger.info("Attempting to create a rollup job $rollupId for index $indexName")

        val indexRollupRequest = IndexRollupRequest(rollup.copy(user = managedIndexMetaData.user), WriteRequest.RefreshPolicy.IMMEDIATE)

        try {
            val response: IndexRollupResponse = client.suspendUntil { execute(IndexRollupAction.INSTANCE, indexRollupRequest, it) }
            logger.info("Received status ${response.status.status} on trying to create rollup job $rollupId")

            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(rollup.id, indexName))
        } catch (e: VersionConflictEngineException) {
            val message = getFailedJobExistsMessage(rollup.id, indexName)
            logger.info(message)
            if (rollupId == previousRunRollupId && hasPreviousRollupAttemptFailed == true) {
                startRollupJob(rollup.id)
            } else {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("info" to message)
            }
        } catch (e: RemoteTransportException) {
            processFailure(rollup.id, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: RemoteTransportException) {
            processFailure(rollup.id, e)
        }

        return this
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetaData.actionMetaData
        return currentMetaData.copy(
            actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(rollupId = rollupId)),
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    fun processFailure(rollupId: String, e: Exception) {
        val message = getFailedMessage(rollupId, indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        info = mapOf("message" to message, "cause" to "${e.message}")
    }

    private suspend fun startRollupJob(rollupId: String) {
        logger.info("Attempting to re-start the job $rollupId")
        try {
            val startRollupRequest = StartRollupRequest(rollupId)
            val response: AcknowledgedResponse = client.suspendUntil { execute(StartRollupAction.INSTANCE, startRollupRequest, it) }
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessRestartMessage(rollupId, indexName))
        } catch (e: Exception) {
            val message = getFailedToStartMessage(rollupId, indexName)
            logger.error(message, e)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message)
        }
    }

    companion object {
        const val name = "attempt_create_rollup"
        fun getFailedMessage(rollupId: String, index: String) = "Failed to create the rollup job [$rollupId] [index=$index]"
        fun getFailedJobExistsMessage(rollupId: String, index: String) = "Rollup job [$rollupId] already exists, skipping creation [index=$index]"
        fun getFailedToStartMessage(rollupId: String, index: String) = "Failed to start the rollup job [$rollupId] [index=$index]"
        fun getSuccessMessage(rollupId: String, index: String) = "Successfully created the rollup job [$rollupId] [index=$index]"
        fun getSuccessRestartMessage(rollupId: String, index: String) = "Successfully restarted the rollup job [$rollupId] [index=$index]"
    }
}
