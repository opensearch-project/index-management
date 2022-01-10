/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.allocation

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData

class AttemptAllocationStep(private val action: AllocationAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        try {
            val response: AcknowledgedResponse = context.client.admin()
                .indices()
                .suspendUntil { updateSettings(UpdateSettingsRequest(buildSettings(), indexName), it) }
            handleResponse(response, indexName)
        } catch (e: Exception) {
            handleException(e, indexName)
        }

        return this
    }

    private fun buildSettings(): Settings {
        val builder = Settings.builder()
        action.require.forEach { (key, value) -> builder.put(SETTINGS_PREFIX + AllocationAction.REQUIRE + "." + key, value) }
        action.include.forEach { (key, value) -> builder.put(SETTINGS_PREFIX + AllocationAction.INCLUDE + "." + key, value) }
        action.exclude.forEach { (key, value) -> builder.put(SETTINGS_PREFIX + AllocationAction.EXCLUDE + "." + key, value) }
        return builder.build()
    }

    private fun handleException(e: Exception, indexName: String) {
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    private fun handleResponse(response: AcknowledgedResponse, indexName: String) {
        if (response.isAcknowledged) {
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(indexName))
        } else {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailedMessage(indexName))
        }
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent() = true

    companion object {
        const val name = "attempt_allocation"
        private const val SETTINGS_PREFIX = "index.routing.allocation."
        fun getFailedMessage(index: String) = "Failed to update allocation setting [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully updated allocation setting [index=$index]"
    }
}
