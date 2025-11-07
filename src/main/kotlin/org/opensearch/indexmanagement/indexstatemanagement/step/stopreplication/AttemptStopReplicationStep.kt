/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.stopreplication

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.commons.replication.ReplicationPluginInterface
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.transport.RemoteTransportException

class AttemptStopReplicationStep : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        try {
            val stopIndexReplicationRequestObj = StopIndexReplicationRequest(indexName)
            val response: AcknowledgedResponse = context.client.suspendUntil {
                val pluginClient = context.client as PluginClient?
                ReplicationPluginInterface.stopReplication(
                    pluginClient!!.innerClient(),
                    stopIndexReplicationRequestObj,
                    it,
                )
            }
            if (response.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getSuccessMessage(indexName))
            } else {
                val message = getFailedMessage(indexName)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
            }
        } catch (e: RemoteTransportException) {
            val cause = ExceptionsHelper.unwrapCause(e)
            if (cause is SnapshotInProgressException) {
                handleSnapshotException(indexName, cause)
            } else {
                handleException(indexName, cause as Exception)
            }
        } catch (e: SnapshotInProgressException) {
            handleSnapshotException(indexName, e)
        } catch (e: Exception) {
            handleException(indexName, e)
        }
        return this
    }

    private fun handleSnapshotException(indexName: String, e: SnapshotInProgressException) {
        val message = getSnapshotMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        info = mapOf("message" to message)
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

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData = currentMetadata.copy(
        stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
        transitionTo = null,
        info = info,
    )

    override fun isIdempotent() = false

    companion object {
        const val name = "attempt_stop_replication"

        fun getFailedMessage(index: String) = "Failed to stop replication [index=$index]"

        fun getSuccessMessage(index: String) = "Successfully stopped replication [index=$index]"

        fun getSnapshotMessage(index: String) = "Index had snapshot in progress, retrying stop replication [index=$index]"
    }
}
