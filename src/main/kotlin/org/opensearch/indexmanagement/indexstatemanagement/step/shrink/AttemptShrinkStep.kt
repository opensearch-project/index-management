/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.admin.indices.shrink.ResizeResponse
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
import org.opensearch.indexmanagement.indexstatemanagement.util.isIndexGreen
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException
import java.lang.Exception

class AttemptShrinkStep(private val action: ShrinkAction) : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount")
    override suspend fun execute(): AttemptShrinkStep {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val actionMetadata = context.metadata.actionMetaData
        val shrinkActionProperties = actionMetadata?.actionProperties?.shrinkActionProperties
        if (shrinkActionProperties == null) {
            info = mapOf("message" to "Shrink action properties are null, metadata was not properly populated")
            stepStatus = StepStatus.FAILED
            return this
        }
        try {
            if (!isIndexGreen(context.client, indexName)) {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to INDEX_HEALTH_NOT_GREEN_MESSAGE)
                return this
            }
            // If the resize index api fails, the step will be set to failed and resizeIndex will return false
            if (!resizeIndex(indexName, shrinkActionProperties, context)) return this
            info = mapOf("message" to getSuccessMessage(shrinkActionProperties.targetIndexName))
            stepStatus = StepStatus.COMPLETED
            return this
        } catch (e: RemoteTransportException) {
            info = mapOf("message" to FAILURE_MESSAGE)
            releaseShrinkLock(shrinkActionProperties, context.jobContext, logger)
            stepStatus = StepStatus.FAILED
            return this
        } catch (e: Exception) {
            releaseShrinkLock(shrinkActionProperties, context.jobContext, logger)
            info = mapOf("message" to FAILURE_MESSAGE, "cause" to "{${e.message}}")
            stepStatus = StepStatus.FAILED
            return this
        }
    }

    private suspend fun resizeIndex(sourceIndex: String, shrinkActionProperties: ShrinkActionProperties, context: StepContext): Boolean {
        val targetIndex = shrinkActionProperties.targetIndexName
        val req = ResizeRequest(targetIndex, sourceIndex)
        req.targetIndexRequest.settings(
            Settings.builder()
                .put(AttemptMoveShardsStep.ROUTING_SETTING, shrinkActionProperties.nodeName)
                .put(INDEX_NUMBER_OF_SHARDS, shrinkActionProperties.targetNumShards)
                .build()
        )
        action.aliases?.forEach { req.targetIndexRequest.alias(it) }
        val resizeResponse: ResizeResponse = context.client.admin().indices().suspendUntil { resizeIndex(req, it) }
        if (!resizeResponse.isAcknowledged) {
            info = mapOf("message" to FAILURE_MESSAGE)
            releaseShrinkLock(shrinkActionProperties, context.jobContext, logger)
            stepStatus = StepStatus.FAILED
            return false
        }
        return true
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetadata.actionMetaData
        return currentMetadata.copy(
            actionMetaData = currentActionMetaData?.copy(),
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent() = false

    companion object {
        const val name = "attempt_shrink_step"
        const val FAILURE_MESSAGE = "Shrink failed when sending shrink request."
        const val INDEX_HEALTH_NOT_GREEN_MESSAGE = "Shrink delayed because index health is not green."
        fun getSuccessMessage(newIndex: String) = "Shrink started. $newIndex currently being populated."
    }
}
