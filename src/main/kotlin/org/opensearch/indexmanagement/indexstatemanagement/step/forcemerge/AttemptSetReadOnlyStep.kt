/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.ForceMergeAction
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException

class AttemptSetReadOnlyStep(private val action: ForceMergeAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("ReturnCount")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val indexSetToReadOnly = setIndexToReadOnly(indexName, context)

        // If setIndexToReadOnly returns false, updating settings failed and failed info was already updated, can return early
        if (!indexSetToReadOnly) return this

        // Complete step since index is read-only
        stepStatus = StepStatus.COMPLETED
        info = mapOf("message" to getSuccessMessage(indexName))

        return this
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun setIndexToReadOnly(indexName: String, context: StepContext): Boolean {
        try {
            val updateSettingsRequest = UpdateSettingsRequest()
                .indices(indexName)
                .settings(Settings.builder().put(SETTING_BLOCKS_WRITE, true))
            val response: AcknowledgedResponse = context.client.admin().indices()
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
            handleException(indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return false
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

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData =
        currentMetadata.copy(
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )

    override fun isIdempotent() = true

    companion object {
        const val name = "attempt_set_read_only"
        fun getFailedMessage(index: String) = "Failed to set index to read-only [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully set index to read-only [index=$index]"
    }
}
