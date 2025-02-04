/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.alias

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.indexstatemanagement.action.AliasAction
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData

class AttemptAliasActionsStep(private val action: AliasAction) : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        try {
            val request = IndicesAliasesRequest()
            action.actions.forEach {
                // Applying the actions on the managed index.
                it.indices(indexName)
                request.addAliasAction(it)
            }
            val response: AcknowledgedResponse = context.client.admin().indices().suspendUntil { aliases(request, it) }
            handleResponse(response, indexName, action.actions)
        } catch (e: Exception) {
            handleException(e, indexName, action.actions)
        }
        return this
    }

    private fun handleException(e: Exception, indexName: String, actions: List<IndicesAliasesRequest.AliasActions>) {
        val message = getFailedMessage(indexName, actions)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    private fun handleResponse(
        response: AcknowledgedResponse,
        indexName: String,
        actions: List<IndicesAliasesRequest.AliasActions>,
    ) {
        if (response.isAcknowledged) {
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSuccessMessage(indexName))
        } else {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailedMessage(indexName, actions))
        }
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData = currentMetadata.copy(
        stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
        transitionTo = null,
        info = info,
    )

    override fun isIdempotent() = true

    companion object {
        val validTopContextFields = setOf("index")
        const val name = "attempt_alias"

        fun getFailedMessage(
            index: String,
            actions: List<IndicesAliasesRequest.AliasActions>,
        ) = "Failed to update alias [index=$index] for actions: [actions=$actions]"

        fun getSuccessMessage(index: String) = "Successfully updated alias [index=$index]"
    }
}
