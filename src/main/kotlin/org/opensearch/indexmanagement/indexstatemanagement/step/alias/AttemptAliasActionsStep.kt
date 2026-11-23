/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.alias

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
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
                // Applying the actions on the managed index. The action is owned by the policy of the managed index
                // config, which is written back to the config index (for example when the job interval changes). The
                // managed index is therefore set on a copy: setting it on the policy's own action would store
                // `indices` in the policy, which the alias action rejects when the config is parsed again.
                request.addAliasAction(it.copyForIndex(indexName))
            }
            val response: AcknowledgedResponse = context.client.admin().indices().suspendUntil { aliases(request, it) }
            handleResponse(response, indexName, action.actions)
        } catch (e: Exception) {
            handleException(e, indexName, action.actions)
        }
        return this
    }

    // Copies the action the same way the policy stores and parses it (see AliasActionParser), which also handles
    // the fields that are not set. Writing it to a stream does not, it fails when `indices` is not set.
    private fun IndicesAliasesRequest.AliasActions.copyForIndex(indexName: String): IndicesAliasesRequest.AliasActions {
        val builder = XContentFactory.jsonBuilder()
        toXContent(builder, ToXContent.EMPTY_PARAMS)
        XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput())
            .use { parser ->
                parser.nextToken()
                return IndicesAliasesRequest.AliasActions.fromXContent(parser).indices(indexName)
            }
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
