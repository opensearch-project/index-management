/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.replicacount

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.ReplicaCountAction
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.transport.RemoteTransportException

class AttemptReplicaCountStep(private val action: ReplicaCountAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private val numOfReplicas = action.numOfReplicas

    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        try {
            val updateSettingsRequest = UpdateSettingsRequest()
                .indices(indexName)
                .settings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, numOfReplicas))
            val response: AcknowledgedResponse = context.client.admin().indices()
                .suspendUntil { updateSettings(updateSettingsRequest, it) }

            if (response.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to getSuccessMessage(indexName, numOfReplicas))
            } else {
                val message = getFailedMessage(indexName, numOfReplicas)
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to message)
            }
        } catch (e: RemoteTransportException) {
            handleException(indexName, numOfReplicas, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(indexName, numOfReplicas, e)
        }

        return this
    }

    private fun handleException(indexName: String, numOfReplicas: Int, e: Exception) {
        val message = getFailedMessage(indexName, numOfReplicas)
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

    override fun isIdempotent() = true

    companion object {
        const val name = "attempt_set_replica_count"
        fun getFailedMessage(index: String, numOfReplicas: Int) = "Failed to set number_of_replicas to $numOfReplicas [index=$index]"
        fun getSuccessMessage(index: String, numOfReplicas: Int) = "Successfully set number_of_replicas to $numOfReplicas [index=$index]"
    }
}
