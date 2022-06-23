/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.getSecurityFailureMessage
import org.opensearch.indexmanagement.indexstatemanagement.util.getUpdatedShrinkActionProperties
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.renewShrinkLock
import org.opensearch.indexmanagement.indexstatemanagement.util.resetReadOnlyAndRouting
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.transport.RemoteTransportException

abstract class ShrinkStep(
    name: String,
    private val cleanupSettings: Boolean,
    private val cleanupLock: Boolean,
    private val cleanupTargetIndex: Boolean
) : Step(name) {
    protected val logger: Logger = LogManager.getLogger(javaClass)
    protected var stepStatus = StepStatus.STARTING
    protected var info: Map<String, Any>? = null
    protected var shrinkActionProperties: ShrinkActionProperties? = null

    @Suppress("ReturnCount")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        try {
            wrappedExecute(context)
        } catch (e: OpenSearchSecurityException) {
            val securityFailureMessage = getSecurityFailureMessage(e.localizedMessage)
            cleanupAndFail(securityFailureMessage, securityFailureMessage, e.message, e)
            return this
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e)
            cleanupAndFail(getGenericFailureMessage(), getGenericFailureMessage(), cause = e.message, e = unwrappedException as java.lang.Exception)
            return this
        } catch (e: Exception) {
            cleanupAndFail(getGenericFailureMessage(), getGenericFailureMessage(), cause = e.message, e = e)
            return this
        }
        return this
    }

    protected suspend fun cleanupAndFail(infoMessage: String, logMessage: String? = null, cause: String? = null, e: Exception? = null) {
        cleanupResources(cleanupSettings, cleanupLock, cleanupTargetIndex)
        fail(infoMessage, logMessage, cause, e)
    }

    abstract fun getGenericFailureMessage(): String

    abstract suspend fun wrappedExecute(context: StepContext): Step

    @Suppress("ReturnCount")
    protected suspend fun updateAndGetShrinkActionProperties(context: StepContext): ShrinkActionProperties? {
        val actionMetadata = context.metadata.actionMetaData
        var localShrinkActionProperties = actionMetadata?.actionProperties?.shrinkActionProperties
        shrinkActionProperties = localShrinkActionProperties
        if (localShrinkActionProperties == null) {
            cleanupAndFail(METADATA_FAILURE_MESSAGE, METADATA_FAILURE_MESSAGE)
            return null
        }
        val lock = renewShrinkLock(localShrinkActionProperties, context.lockService, logger)
        if (lock == null) {
            cleanupAndFail(
                "Failed to renew lock on node [${localShrinkActionProperties.nodeName}]",
                "Shrink action failed to renew lock on node [${localShrinkActionProperties.nodeName}]"
            )
            return null
        }
        // After renewing the lock we need to update the primary term and sequence number
        localShrinkActionProperties = getUpdatedShrinkActionProperties(localShrinkActionProperties, lock)
        shrinkActionProperties = localShrinkActionProperties
        return localShrinkActionProperties
    }

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
        val localShrinkActionProperties = shrinkActionProperties
        if (localShrinkActionProperties != null) {
            if (resetSettings) resetIndexSettings(localShrinkActionProperties)
            if (deleteTargetIndex) deleteTargetIndex(localShrinkActionProperties)
            if (releaseLock) releaseLock(localShrinkActionProperties)
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

    @Suppress("NestedBlockDepth")
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
                logger.error(
                    "Shrink action failed to delete target index [$targetIndexName] after a failure due to a null client in the step context"
                )
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
                logger.error(
                    "Shrink action failed to release lock on node [${shrinkActionProperties.nodeName}] due to uninitialized metadata values."
                )
            }
        } catch (e: Exception) {
            logger.error("Failed to release Shrink action lock on node [${shrinkActionProperties.nodeName}]: $e")
        }
    }

    companion object {
        const val METADATA_FAILURE_MESSAGE = "Shrink action properties are null, metadata was not properly populated"
    }
}
