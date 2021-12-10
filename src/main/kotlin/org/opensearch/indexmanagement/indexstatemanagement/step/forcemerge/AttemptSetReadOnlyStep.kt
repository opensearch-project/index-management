/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ForceMergeActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.transport.RemoteTransportException

class AttemptSetReadOnlyStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ForceMergeActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    override suspend fun execute(): AttemptSetReadOnlyStep {
        val indexSetToReadOnly = setIndexToReadOnly(indexName)

        // If setIndexToReadOnly returns false, updating settings failed and failed info was already updated, can return early
        if (!indexSetToReadOnly) return this

        // Complete step since index is read-only
        stepStatus = StepStatus.COMPLETED
        info = mapOf("message" to getSuccessMessage(indexName))

        return this
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun setIndexToReadOnly(indexName: String): Boolean {
        try {
            val updateSettingsRequest = UpdateSettingsRequest()
                .indices(indexName)
                .settings(Settings.builder().put(SETTING_BLOCKS_WRITE, true))
            val response: AcknowledgedResponse = client.admin().indices()
                .suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                return true
            }

            // If response is not acknowledged, then add failed info
            val message = getFailedMessage(indexName)
            logger.warn(message)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message)
        } catch (e: RemoteTransportException) {
            handleException(ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(e)
        }

        return false
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

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData =
        currentMetaData.copy(stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus), transitionTo = null, info = info)

    companion object {
        const val name = "attempt_set_read_only"
        fun getFailedMessage(index: String) = "Failed to set index to read-only [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully set index to read-only [index=$index]"
    }
}
