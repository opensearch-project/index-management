/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.notification

import org.apache.logging.log4j.LogManager
import org.opensearch.indexmanagement.indexstatemanagement.action.NotificationAction
import org.opensearch.indexmanagement.indexstatemanagement.util.publishLegacyNotification
import org.opensearch.indexmanagement.indexstatemanagement.util.sendNotification
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript

class AttemptNotificationStep(private val action: NotificationAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val scriptService = context.scriptService
        try {
            val compiledMessage = compileTemplate(scriptService, action.messageTemplate, context.metadata)
            action.destination?.buildLegacyBaseMessage(null, compiledMessage)?.publishLegacyNotification(context.client)
            action.channel?.sendNotification(context.client, CHANNEL_TITLE, context.metadata, compiledMessage, context.user)
            // publish and send throws an error for any invalid responses so its safe to assume if we reach this point it was successful
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(indexName))
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

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    private fun compileTemplate(scriptService: ScriptService, template: Script, managedIndexMetaData: ManagedIndexMetaData): String {
        return scriptService.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to managedIndexMetaData.convertToMap()))
            .execute()
    }

    override fun isIdempotent(): Boolean = false

    companion object {
        const val name = "attempt_notification"
        const val CHANNEL_TITLE = "Index Management-ISM-Notification Action"
        fun getFailedMessage(index: String) = "Failed to send notification [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully sent notification [index=$index]"
    }
}
