/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ForceMergeActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.opensearchapi.getUsefulCauseString
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.rest.RestStatus
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

class AttemptCallForceMergeStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ForceMergeActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step(name, managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = false

    @Suppress("TooGenericExceptionCaught", "ComplexMethod")
    override suspend fun execute(): AttemptCallForceMergeStep {
        try {

            val startTime = Instant.now().toEpochMilli()
            val request = ForceMergeRequest(indexName).maxNumSegments(config.maxNumSegments)
            var response: ForceMergeResponse? = null
            var throwable: Throwable? = null
            GlobalScope.launch(Dispatchers.IO + CoroutineName("ISM-ForceMerge-$indexName")) {
                try {
                    response = client.admin().indices().suspendUntil { forceMerge(request, it) }
                    if (response?.status == RestStatus.OK) {
                        logger.info(getSuccessMessage(indexName))
                    } else {
                        logger.warn(getFailedMessage(indexName))
                    }
                } catch (t: Throwable) {
                    throwable = t
                }
            }

            while (response == null && (Instant.now().toEpochMilli() - startTime) < FIVE_MINUTES_IN_MILLIS) {
                delay(FIVE_SECONDS_IN_MILLIS)
                throwable?.let { throw it }
            }

            val shadowedResponse = response
            if (shadowedResponse?.let { it.status == RestStatus.OK } != false) {
                stepStatus = StepStatus.COMPLETED
                info = mapOf("message" to if (shadowedResponse == null) getSuccessfulCallMessage(indexName) else getSuccessMessage(indexName))
            } else {
                // Otherwise the request to force merge encountered some problem
                stepStatus = StepStatus.FAILED
                info = mapOf(
                    "message" to getFailedMessage(indexName),
                    "status" to shadowedResponse.status,
                    "shard_failures" to shadowedResponse.shardFailures.map { it.getUsefulCauseString() }
                )
            }
        } catch (e: RemoteTransportException) {
            handleException(ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(e)
        }

        return this
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
        // Saving maxNumSegments in ActionProperties after the force merge operation has begun so that if a ChangePolicy occurred
        // in between this step and WaitForForceMergeStep, a cached segment count expected from the operation is available
        val currentActionMetaData = currentMetaData.actionMetaData
        return currentMetaData.copy(
            actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(maxNumSegments = config.maxNumSegments)),
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "attempt_call_force_merge"
        const val FIVE_MINUTES_IN_MILLIS = 1000 * 60 * 5 // how long to wait for the force merge request before moving on
        const val FIVE_SECONDS_IN_MILLIS = 1000L * 5L // delay
        fun getFailedMessage(index: String) = "Failed to start force merge [index=$index]"
        fun getSuccessfulCallMessage(index: String) = "Successfully called force merge [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully completed force merge [index=$index]"
    }
}
