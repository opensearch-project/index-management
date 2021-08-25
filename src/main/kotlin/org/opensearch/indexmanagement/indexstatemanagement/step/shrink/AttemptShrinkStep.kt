/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.indices.shrink.ResizeRequest
import org.opensearch.action.admin.indices.shrink.ResizeResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ShrinkActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_NUMBER_OF_SHARDS
import org.opensearch.indexmanagement.indexstatemanagement.util.releaseShrinkLock
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.transport.RemoteTransportException

class AttemptShrinkStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ShrinkActionConfig,
    managedIndexMetaData: ManagedIndexMetaData,
    val context: JobExecutionContext
) : Step(name, managedIndexMetaData) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = false

    @Suppress("TooGenericExceptionCaught", "ComplexMethod")
    override suspend fun execute(): AttemptShrinkStep {
        try {
            val healthReq = ClusterHealthRequest().indices(managedIndexMetaData.index).waitForGreenStatus()
            val response: ClusterHealthResponse = client.admin().cluster().suspendUntil { health(healthReq, it) }
            // check status of cluster health
            if (response.isTimedOut) {
                stepStatus = StepStatus.CONDITION_NOT_MET
                info = mapOf("message" to getIndexHealthNotGreenMessage(managedIndexMetaData.index))
                return this
            }
            val targetIndexName = managedIndexMetaData.actionMetaData!!.actionProperties!!.shrinkActionProperties!!.targetIndexName
            val aliases = config.aliases
            val req = ResizeRequest(targetIndexName, managedIndexMetaData.index)
            req.targetIndexRequest.settings(
                Settings.builder()
                    .put(INDEX_NUMBER_OF_SHARDS, managedIndexMetaData.actionMetaData.actionProperties!!.shrinkActionProperties!!.targetNumShards!!)
                    .build()
            )
            aliases?.forEach { req.targetIndexRequest.alias(it) }
            val resizeResponse: ResizeResponse = client.admin().indices().suspendUntil { resizeIndex(req, it) }
            if (!resizeResponse.isAcknowledged) {
                info = mapOf("message" to getFailureMessage(managedIndexMetaData.index))
                releaseShrinkLock(managedIndexMetaData, context, logger)
                stepStatus = StepStatus.FAILED
                return this
            }
            info = mapOf("message" to getSuccessMessage(managedIndexMetaData.index, targetIndexName!!))
            stepStatus = StepStatus.COMPLETED
            return this
        } catch (e: RemoteTransportException) {
            info = mapOf("message" to getFailureMessage(managedIndexMetaData.index))
            releaseShrinkLock(managedIndexMetaData, context, logger)
            stepStatus = StepStatus.FAILED
            return this
        }
    }

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetaData.actionMetaData
        return currentMetaData.copy(
            actionMetaData = currentActionMetaData?.copy(),
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        const val name = "attempt_shrink_step"
        fun getSuccessMessage(index: String, newIndex: String) = "Shrink started on $index. $newIndex currently being populated."
        fun getIndexHealthNotGreenMessage(index: String) = "Shrink delayed because $index health is not green."
        fun getFailureMessage(index: String) = "Shrink failed on $index when sending shrink request."
    }
}
