/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.resetReadOnlyAndRouting
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties

abstract class ShrinkStep(name: String) : Step(name) {
    protected val logger: Logger = LogManager.getLogger(javaClass)
    protected var stepStatus = StepStatus.STARTING
    protected var info: Map<String, Any>? = null
    protected var shrinkActionProperties: ShrinkActionProperties? = null

    protected fun fail(infoMessage: String, logMessage: String? = null, cause: String? = null, e: Exception? = null) {
        if (logMessage != null) {
            if (e != null) {
                logger.error(logMessage, e)
            } else {
                logger.error(logMessage)
            }
        }
        info = if (cause == null) mapOf("message" to infoMessage) else mapOf("message" to infoMessage, "cause" to cause)
        stepStatus = StepStatus.FAILED
        shrinkActionProperties = null
    }

    protected suspend fun cleanupResources(resetSettings: Boolean, releaseLock: Boolean, deleteTargetIndex: Boolean) {
        val shrinkActionProperties = context?.metadata?.actionMetaData?.actionProperties?.shrinkActionProperties
        if (shrinkActionProperties != null) {
            if (resetSettings) resetIndexSettings(shrinkActionProperties)
            if (deleteTargetIndex) deleteTargetIndex(shrinkActionProperties)
            if (releaseLock) releaseLock(shrinkActionProperties)
        } else {
            logger.error("Shrink action failed to clean up resources due to null shrink action properties.")
        }
    }

    private suspend fun resetIndexSettings(shrinkActionProperties: ShrinkActionProperties) {
        val originalIndexSettings = shrinkActionProperties.originalIndexSettings
        val indexName = context?.metadata?.index
        val client = context?.client
        try {
            if (indexName != null && client != null) {
                val reset = resetReadOnlyAndRouting(indexName, client, originalIndexSettings)
                if (!reset) logger.error("Shrink action failed to reset index settings on [$indexName]")
            } else {
                logger.error("Shrink action failed to reset index settings on [$indexName] due to uninitialized metadata values.")
            }
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to clean up routing and readonly setting on [$indexName] due to failure: $e")
        }
    }

    private suspend fun deleteTargetIndex(shrinkActionProperties: ShrinkActionProperties) {
        val client = context?.client
        val targetIndexName = shrinkActionProperties.targetIndexName
        try {
            if (client != null) {
                // Use plugin level permissions when deleting the failed target shrink index after a failure
                client.threadPool().threadContext.stashContext().use {
                    val deleteRequest = DeleteIndexRequest(targetIndexName)
                    val response: AcknowledgedResponse =
                        client.admin().indices().suspendUntil { delete(deleteRequest, it) }
                    if (!response.isAcknowledged) {
                        logger.error("Shrink action failed to delete target index [$targetIndexName] during cleanup after a failure")
                    }
                }
            } else {
                logger.error("Shrink action failed to delete target index [$targetIndexName] after a failure due to a null client in the step context")
            }
        } catch (e: Exception) {
            logger.error("Shrink action failed while trying to delete the target index [$targetIndexName] after a failure: $e")
        }
    }

    private suspend fun releaseLock(shrinkActionProperties: ShrinkActionProperties) {
        val lockService = context?.lockService
        try {
            if (lockService != null) {
                val released = releaseShrinkLock(shrinkActionProperties, lockService)
                if (!released) logger.error("Failed to release Shrink action lock on node [${shrinkActionProperties.nodeName}]")
            } else {
                logger.error("Shrink action failed to release lock on node [${shrinkActionProperties.nodeName}] due to uninitialized metadata values.")
            }
        } catch (e: Exception) {
            logger.error("Failed to release Shrink action lock on node [${shrinkActionProperties.nodeName}]: $e")
        }
    }

    companion object {
        const val METADATA_FAILURE_MESSAGE = "Shrink action properties are null, metadata was not properly populated"
    }
}
