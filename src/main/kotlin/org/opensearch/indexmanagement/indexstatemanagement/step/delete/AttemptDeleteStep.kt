/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.delete

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.DeleteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.transport.RemoteTransportException
import java.lang.Exception

class AttemptDeleteStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: DeleteActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = true

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute(): AttemptDeleteStep {
        try {
            val response: AcknowledgedResponse = client.admin().indices()
                .suspendUntil { delete(DeleteIndexRequest(indexName), it) }

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
                handleSnapshotException(cause)
            } else {
                handleException(cause as Exception)
            }
        } catch (e: SnapshotInProgressException) {
            handleSnapshotException(e)
        } catch (e: Exception) {
            handleException(e)
        }

        return this
    }

    private fun handleSnapshotException(e: SnapshotInProgressException) {
        val message = getSnapshotMessage(indexName)
        logger.warn(message, e)
        stepStatus = StepStatus.CONDITION_NOT_MET
        info = mapOf("message" to message)
    }

    private fun handleException(e: Exception) {
        val message = getFailedMessage(indexName)
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetaData.copy(
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "attempt_delete"
        fun getFailedMessage(index: String) = "Failed to delete index [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully deleted index [index=$index]"
        fun getSnapshotMessage(index: String) = "Index had snapshot in progress, retrying deletion [index=$index]"
    }
}
