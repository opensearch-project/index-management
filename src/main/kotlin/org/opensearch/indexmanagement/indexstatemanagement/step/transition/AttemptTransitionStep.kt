/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.transition

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.action.TransitionsAction
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getOldestRolloverTime
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.evaluateConditions
import org.opensearch.indexmanagement.indexstatemanagement.util.hasStatsConditions
import org.opensearch.indexmanagement.opensearchapi.getUsefulCauseString
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.rest.RestStatus
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

class AttemptTransitionStep(private val action: TransitionsAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stateName: String? = null
    private var stepStatus = StepStatus.STARTING
    private var policyCompleted: Boolean = false
    private var info: Map<String, Any>? = null

    @Suppress("ReturnCount", "ComplexMethod", "LongMethod", "NestedBlockDepth")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val clusterService = context.clusterService
        val transitions = action.transitions
        val indexMetadataProvider = action.indexMetadataProvider
        try {
            if (transitions.isEmpty()) {
                logger.info("$indexName transitions are empty, completing policy")
                policyCompleted = true
                stepStatus = StepStatus.COMPLETED
                return this
            }

            val indexMetadata = clusterService.state().metadata().index(indexName)
            val inCluster = clusterService.state().metadata().hasIndex(indexName) && indexMetadata?.indexUUID == context.metadata.indexUuid

            val indexCreationDate = getIndexCreationDate(context.metadata, indexMetadataProvider, clusterService, indexName, inCluster)
            val indexCreationDateInstant = Instant.ofEpochMilli(indexCreationDate)
            if (indexCreationDate == -1L) {
                logger.warn("$indexName had an indexCreationDate=-1L, cannot use for comparison")
            }
            val stepStartTime = getStepStartTime(context.metadata)
            var numDocs: Long? = null
            var indexSize: ByteSizeValue? = null

            val rolloverDate: Instant? = if (inCluster) indexMetadata.getOldestRolloverTime() else null
            if (transitions.any { it.conditions?.rolloverAge !== null }) {
                // if we have a transition with rollover age condition, then we must have a rollover date
                // otherwise fail this transition
                if (rolloverDate == null) {
                    val message = getFailedRolloverDateMessage(indexName)
                    logger.warn(message)
                    stepStatus = StepStatus.FAILED
                    info = mapOf("message" to message)
                    return this
                }
            }

            if (transitions.any { it.hasStatsConditions() }) {
                if (inCluster) {
                    val statsRequest = IndicesStatsRequest()
                        .indices(indexName).clear().docs(true)
                    val statsResponse: IndicesStatsResponse =
                        context.client.admin().indices().suspendUntil { stats(statsRequest, it) }

                    if (statsResponse.status != RestStatus.OK) {
                        val message = getFailedStatsMessage(indexName)
                        logger.warn("$message - ${statsResponse.status}")
                        stepStatus = StepStatus.FAILED
                        info = mapOf(
                            "message" to message,
                            "shard_failures" to statsResponse.shardFailures.map { it.getUsefulCauseString() }
                        )
                        return this
                    }
                    numDocs = statsResponse.primaries.getDocs()?.count ?: 0
                    indexSize = ByteSizeValue(statsResponse.primaries.getDocs()?.totalSizeInBytes ?: 0)
                } else {
                    logger.warn("Cannot use index size/doc count transition conditions for index [$indexName] that does not exist in cluster")
                }
            }

            // Find the first transition that evaluates to true and get the state to transition to, otherwise return null if none are true
            stateName = transitions.find {
                it.evaluateConditions(indexCreationDateInstant, numDocs, indexSize, stepStartTime, rolloverDate)
            }?.stateName
            val message: String
            val stateName = stateName // shadowed on purpose to prevent var from changing
            if (stateName != null) {
                logger.info(
                    "$indexName transition conditions evaluated to true [indexCreationDate=$indexCreationDate," +
                        " numDocs=$numDocs, indexSize=${indexSize?.bytes},stepStartTime=${stepStartTime.toEpochMilli()}]"
                )
                stepStatus = StepStatus.COMPLETED
                message = getSuccessMessage(indexName, stateName)
            } else {
                stepStatus = StepStatus.CONDITION_NOT_MET
                message = getEvaluatingMessage(indexName)
            }
            info = mapOf("message" to message)
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
        return currentMetadata.copy(
            policyCompleted = policyCompleted,
            transitionTo = stateName,
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            info = info
        )
    }

    @Suppress("ReturnCount")
    private suspend fun getIndexCreationDate(
        metadata: ManagedIndexMetaData,
        indexMetadataProvider: IndexMetadataProvider,
        clusterService: ClusterService,
        indexName: String,
        inCluster: Boolean
    ): Long {
        try {
            // If we do have an index creation date cached already then use that
            metadata.indexCreationDate?.let { return it }
            // Otherwise, check if this index is in cluster state first
            return if (inCluster) {
                val clusterStateMetadata = clusterService.state().metadata()
                if (clusterStateMetadata.hasIndex(indexName)) clusterStateMetadata.index(indexName).creationDate else -1L
            } else {
                // And then finally check all other index types which may not be in the cluster
                val nonDefaultIndexTypes = indexMetadataProvider.services.keys.filter { it != DEFAULT_INDEX_TYPE }
                val multiTypeIndexNameToMetaData = indexMetadataProvider.getMultiTypeISMIndexMetadata(nonDefaultIndexTypes, listOf(indexName))
                // the managedIndexConfig.indexUuid should be unique across all index types
                val indexCreationDate = multiTypeIndexNameToMetaData.values.firstOrNull {
                    it[indexName]?.indexUuid == metadata.indexUuid
                }?.get(indexName)?.indexCreationDate
                indexCreationDate ?: -1
            }
        } catch (e: Exception) {
            logger.error("Failed to get index creation date for $indexName", e)
        }
        // -1L index age is ignored during condition checks
        return -1L
    }

    override fun isIdempotent() = true

    companion object {
        const val name = "attempt_transition_step"
        fun getFailedMessage(index: String) = "Failed to transition index [index=$index]"
        fun getFailedStatsMessage(index: String) = "Failed to get stats information for the index [index=$index]"
        fun getFailedRolloverDateMessage(index: String) =
            "Failed to transition index as min_rollover_age condition was used, but the index has never been rolled over [index=$index]"
        fun getEvaluatingMessage(index: String) = "Evaluating transition conditions [index=$index]"
        fun getSuccessMessage(index: String, state: String) = "Transitioning to $state [index=$index]"
    }
}
