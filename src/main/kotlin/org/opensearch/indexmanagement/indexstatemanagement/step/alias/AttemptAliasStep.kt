/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.alias

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.common.Strings
import org.opensearch.indexmanagement.indexstatemanagement.action.AliasAction
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript

class AttemptAliasStep(private val action: AliasAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        try {
            val request = IndicesAliasesRequest()
            action.actions.forEach {
                // Take the indices that were set to be updated on the alias action and join them into a single string
                // So we don't have to pass each one individually into the script service to be compiled
                val indices = it.indices().joinToString(",")
                // Compile them so the user can use the current dynamic index name in the name such as "{{ctx.index}}" in the static policy
                val indicesScript = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, indices, mapOf())
                val compiledIndices = compileTemplate(indicesScript, context.metadata, indices, context.scriptService)
                // Split them back into individual index names and set back on the AliasActions request
                val splitIndices = Strings.splitStringByCommaToArray(compiledIndices)
                it.indices(*splitIndices)
                request.addAliasAction(it)
            }
            val response: AcknowledgedResponse = context.client.admin().indices().suspendUntil { aliases(request, it) }
            handleResponse(response, indexName)
        } catch (e: Exception) {
            handleException(e, indexName)
        }
        return this
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

    private fun compileTemplate(
        template: Script,
        managedIndexMetaData: ManagedIndexMetaData,
        defaultValue: String,
        scriptService: ScriptService
    ): String {
        val contextMap = managedIndexMetaData.convertToMap().filterKeys { key -> key in validTopContextFields }
        val compiledValue = scriptService.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to contextMap))
            .execute()
        return compiledValue.ifBlank { defaultValue }
    }

    override fun isIdempotent() = true

    companion object {
        val validTopContextFields = setOf("index")
        const val name = "attempt_alias"
        fun getFailedMessage(index: String) = "Failed to update alias [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully updated alias [index=$index]"
    }
}
