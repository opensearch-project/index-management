/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.admin.indices.shrink.ResizeResponse
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.getSecurityFailureMessage
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
import org.opensearch.indexmanagement.indexstatemanagement.util.resetReadOnlyAndRouting
import org.opensearch.indexmanagement.indexstatemanagement.util.getNodeFreeMemoryAfterShrink
import org.opensearch.indexmanagement.indexstatemanagement.util.isIndexGreen
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.renewShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.getUpdatedShrinkActionProperties
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
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
    private var shrinkActionProperties: ShrinkActionProperties? = null

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount")
    override suspend fun execute(): AttemptShrinkStep {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val actionMetadata = context.metadata.actionMetaData
        val localShrinkActionProperties = actionMetadata?.actionProperties?.shrinkActionProperties
        shrinkActionProperties = localShrinkActionProperties
        if (localShrinkActionProperties == null) {
            logger.error(WaitForMoveShardsStep.METADATA_FAILURE_MESSAGE)
            cleanupAndFail(WaitForMoveShardsStep.METADATA_FAILURE_MESSAGE)
            return this
        }
        val lock = renewShrinkLock(localShrinkActionProperties, context.lockService, logger)
        if (lock == null) {
            logger.error("Shrink action failed to renew lock on node [${localShrinkActionProperties.nodeName}]")
            cleanupAndFail("Failed to renew lock on node [${localShrinkActionProperties.nodeName}]")
            return this
        }
        shrinkActionProperties = getUpdatedShrinkActionProperties(localShrinkActionProperties, lock)
        try {
            if (!isIndexGreen(context.client, indexName)) {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to INDEX_HEALTH_NOT_GREEN_MESSAGE)
                return this
            }
            if (!isNodeStillSuitable(localShrinkActionProperties.nodeName, indexName, context)) return this

            // If the resize index api fails, the step will be set to failed and resizeIndex will return false
            if (!resizeIndex(indexName, localShrinkActionProperties, context)) return this
            info = mapOf("message" to getSuccessMessage(localShrinkActionProperties.targetIndexName))
            stepStatus = StepStatus.COMPLETED
            return this
        } catch (e: OpenSearchSecurityException) {
            cleanupAndFail(getSecurityFailureMessage(e.localizedMessage), e.message, e)
            return this
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e)
            cleanupAndFail(FAILURE_MESSAGE, cause = e.message, e = unwrappedException as Exception)
            return this
        } catch (e: Exception) {
            cleanupAndFail(FAILURE_MESSAGE, e.message, e)
            return this
        }
    }

    // Sets the action to failed, clears the readonly and allocation settings on the source index, and releases the shrink lock
    private suspend fun cleanupAndFail(message: String, cause: String? = null, e: Exception? = null) {
        e?.let { logger.error(message, e) }
        info = if (cause == null) mapOf("message" to message) else mapOf("message" to message, "cause" to cause)
        stepStatus = StepStatus.FAILED
        // Non-null assertion !! is used to throw an exception on null which would just be caught and logged
        try {
            resetReadOnlyAndRouting(context!!.metadata.index, context!!.client, shrinkActionProperties!!.originalIndexSettings)
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to clean up routing and readonly setting after a failure: $e")
        }
        try {
            releaseShrinkLock(shrinkActionProperties!!, context!!.lockService, logger)
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to release the node lock after a failure: $e")
        }
        shrinkActionProperties = null
    }

    @Suppress("ReturnCount")
    private suspend fun isNodeStillSuitable(nodeName: String, indexName: String, context: StepContext): Boolean {
        // Get the size of the index
        val statsRequest = IndicesStatsRequest().indices(indexName)
        val statsResponse: IndicesStatsResponse = context.client.admin().indices().suspendUntil {
            stats(statsRequest, it)
        }
        val statsStore = statsResponse.total.store
        if (statsStore == null) {
            logger.error("Shrink action failed as indices stats request was missing store stats.")
            cleanupAndFail(FAILURE_MESSAGE)
            return false
        }
        val indexSizeInBytes = statsStore.sizeInBytes
        // Get the remaining memory in the node
        val nodesStatsReq = NodesStatsRequest().addMetric(AttemptMoveShardsStep.OS_METRIC)
        val nodeStatsResponse: NodesStatsResponse = context.client.admin().cluster().suspendUntil { nodesStats(nodesStatsReq, it) }
        // If the node has been replaced, this will fail
        val node = nodeStatsResponse.nodes.firstOrNull { it.node.name == nodeName }
        if (node == null) {
            logger.error("Shrink action failed as node stats were missing the previously selected node.")
            cleanupAndFail(FAILURE_MESSAGE)
            return false
        }
        val remainingMem = getNodeFreeMemoryAfterShrink(node, indexSizeInBytes, context.clusterService.clusterSettings)
        if (remainingMem < 1L) {
            logger.error("Shrink action failed as the previously selected node no longer has enough free space.")
            cleanupAndFail(NOT_ENOUGH_SPACE_FAILURE_MESSAGE)
            return false
        }
        return true
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
            logger.error("Shrink action failed as the resize index request was not acknowledged.")
            cleanupAndFail(FAILURE_MESSAGE)
            return false
        }
        return true
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            actionMetaData = currentMetadata.actionMetaData?.copy(
                actionProperties = ActionProperties(
                    shrinkActionProperties = shrinkActionProperties
                )
            ),
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent() = false

    companion object {
        const val name = "attempt_shrink_step"
        const val FAILURE_MESSAGE = "Shrink failed when sending shrink request."
        const val NOT_ENOUGH_SPACE_FAILURE_MESSAGE = "Shrink failed as the selected node no longer had enough free space to shrink to."
        const val INDEX_HEALTH_NOT_GREEN_MESSAGE = "Shrink delayed because index health is not green."
        fun getSuccessMessage(newIndex: String) = "Shrink started. $newIndex currently being populated."
    }
}
