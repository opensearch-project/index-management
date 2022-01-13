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
import org.opensearch.indexmanagement.indexstatemanagement.action.ForceMergeAction
import org.opensearch.indexmanagement.opensearchapi.getUsefulCauseString
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.rest.RestStatus
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

class AttemptCallForceMergeStep(private val action: ForceMergeAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    @Suppress("TooGenericExceptionCaught", "ComplexMethod")
    override suspend fun execute(): AttemptCallForceMergeStep {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        try {

            val startTime = Instant.now().toEpochMilli()
            val request = ForceMergeRequest(indexName).maxNumSegments(action.maxNumSegments)
            var response: ForceMergeResponse? = null
            var throwable: Throwable? = null
            GlobalScope.launch(Dispatchers.IO + CoroutineName("ISM-ForceMerge-$indexName")) {
                try {
                    response = context.client.admin().indices().suspendUntil { forceMerge(request, it) }
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
            handleException(indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(indexName, e)
        }

        return this
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

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        // Saving maxNumSegments in ActionProperties after the force merge operation has begun so that if a ChangePolicy occurred
        // in between this step and WaitForForceMergeStep, a cached segment count expected from the operation is available
        val currentActionMetaData = currentMetadata.actionMetaData
        return currentMetadata.copy(
            actionMetaData = currentActionMetaData?.copy(actionProperties = ActionProperties(maxNumSegments = action.maxNumSegments)),
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent() = false

    companion object {
        const val name = "attempt_call_force_merge"
        const val FIVE_MINUTES_IN_MILLIS = 1000 * 60 * 5 // how long to wait for the force merge request before moving on
        const val FIVE_SECONDS_IN_MILLIS = 1000L * 5L // delay
        fun getFailedMessage(index: String) = "Failed to start force merge [index=$index]"
        fun getSuccessfulCallMessage(index: String) = "Successfully called force merge [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully completed force merge [index=$index]"
    }
}
