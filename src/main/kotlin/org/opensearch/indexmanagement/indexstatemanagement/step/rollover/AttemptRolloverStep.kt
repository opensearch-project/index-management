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

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.rollover

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.rollover.RolloverRequest
import org.opensearch.action.admin.indices.rollover.RolloverResponse
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.RolloverActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getRolloverAlias
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.util.evaluateConditions
import org.opensearch.indexmanagement.opensearchapi.getUsefulCauseString
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.rest.RestStatus
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

@Suppress("ReturnCount", "TooGenericExceptionCaught")
class AttemptRolloverStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: RolloverActionConfig,
    managedIndexMetaData: ManagedIndexMetaData
) : Step("attempt_rollover", managedIndexMetaData) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null

    override fun isIdempotent() = false

    @Suppress("TooGenericExceptionCaught")
    override suspend fun execute(): AttemptRolloverStep {
        // If we have already rolled over this index then fail as we only allow an index to be rolled over once
        if (managedIndexMetaData.rolledOver == true) {
            logger.warn("$indexName was already rolled over, cannot execute rollover step")
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailedDuplicateRolloverMessage(indexName))
            return this
        }

        val (rolloverTarget, isDataStream) = getRolloverTargetOrUpdateInfo()
        // If the rolloverTarget is null, we would've already updated the failed info from getRolloverTargetOrUpdateInfo and can return early
        rolloverTarget ?: return this

        val statsResponse = getIndexStatsOrUpdateInfo()
        // If statsResponse is null we already updated failed info from getIndexStatsOrUpdateInfo and can return early
        statsResponse ?: return this

        val indexCreationDate = clusterService.state().metadata().index(indexName).creationDate
        val indexAgeTimeValue = if (indexCreationDate == -1L) {
            logger.warn("$indexName had an indexCreationDate=-1L, cannot use for comparison")
            // since we cannot use for comparison, we can set it to 0 as minAge will never be <= 0
            TimeValue.timeValueMillis(0)
        } else {
            TimeValue.timeValueMillis(Instant.now().toEpochMilli() - indexCreationDate)
        }
        val numDocs = statsResponse.primaries.docs?.count ?: 0
        val indexSize = ByteSizeValue(statsResponse.primaries.docs?.totalSizeInBytes ?: 0)
        val conditions = listOfNotNull(
            config.minAge?.let {
                RolloverActionConfig.MIN_INDEX_AGE_FIELD to mapOf(
                    "condition" to it.toString(),
                    "current" to indexAgeTimeValue.toString(),
                    "creationDate" to indexCreationDate
                )
            },
            config.minDocs?.let {
                RolloverActionConfig.MIN_DOC_COUNT_FIELD to mapOf(
                    "condition" to it,
                    "current" to numDocs
                )
            },
            config.minSize?.let {
                RolloverActionConfig.MIN_SIZE_FIELD to mapOf(
                    "condition" to it.toString(),
                    "current" to indexSize.toString()
                )
            }
        ).toMap()

        if (config.evaluateConditions(indexAgeTimeValue, numDocs, indexSize)) {
            logger.info(
                "$indexName rollover conditions evaluated to true [indexCreationDate=$indexCreationDate," +
                    " numDocs=$numDocs, indexSize=${indexSize.bytes}]"
            )
            executeRollover(rolloverTarget, isDataStream, conditions)
        } else {
            stepStatus = StepStatus.CONDITION_NOT_MET
            info = mapOf("message" to getPendingMessage(indexName), "conditions" to conditions)
        }

        return this
    }

    @Suppress("ComplexMethod")
    private suspend fun executeRollover(rolloverTarget: String, isDataStream: Boolean, conditions: Map<String, Map<String, Any?>>) {
        try {
            val request = RolloverRequest(rolloverTarget, null)
            val response: RolloverResponse = client.admin().indices().suspendUntil { rolloverIndex(request, it) }

            // Do not need to check for isRolledOver as we are not passing any conditions or dryrun=true
            // which are the only two ways it comes back as false

            // If the response is acknowledged, then the new index is created and added to one of the following index abstractions:
            // 1. IndexAbstraction.Type.DATA_STREAM - the new index is added to the data stream indicated by the 'rolloverTarget'
            // 2. IndexAbstraction.Type.ALIAS - the new index is added to the alias indicated by the 'rolloverTarget'
            if (response.isAcknowledged) {
                val message = when {
                    isDataStream -> getSuccessDataStreamRolloverMessage(rolloverTarget, indexName)
                    else -> getSuccessMessage(indexName)
                }

                stepStatus = StepStatus.COMPLETED
                info = listOfNotNull(
                    "message" to message,
                    if (conditions.isEmpty()) null else "conditions" to conditions // don't show empty conditions object if no conditions specified
                ).toMap()
            } else {
                val message = when {
                    isDataStream -> getFailedDataStreamRolloverMessage(rolloverTarget)

                    // If the alias update response was NOT acknowledged, then the new index was created but we failed to swap the alias
                    else -> getFailedAliasUpdateMessage(indexName, response.newIndex)
                }
                logger.warn(message)
                stepStatus = StepStatus.FAILED
                info = listOfNotNull(
                    "message" to message,
                    if (conditions.isEmpty()) null else "conditions" to conditions // don't show empty conditions object if no conditions specified
                ).toMap()
            }
        } catch (e: RemoteTransportException) {
            handleException(ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(e)
        }
    }

    private fun getRolloverTargetOrUpdateInfo(): Pair<String?, Boolean> {
        val metadata = clusterService.state().metadata()
        val indexAbstraction = metadata.indicesLookup[indexName]
        val isDataStreamIndex = indexAbstraction?.parentDataStream != null

        val rolloverTarget = when {
            isDataStreamIndex -> indexAbstraction?.parentDataStream?.name
            else -> metadata.index(indexName).getRolloverAlias()
        }

        if (rolloverTarget == null) {
            val message = getFailedNoValidAliasMessage(indexName)
            logger.warn(message)
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to message)
        }

        return rolloverTarget to isDataStreamIndex
    }

    private suspend fun getIndexStatsOrUpdateInfo(): IndicesStatsResponse? {
        try {
            val statsRequest = IndicesStatsRequest()
                .indices(indexName).clear().docs(true)
            val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil { stats(statsRequest, it) }

            if (statsResponse.status == RestStatus.OK) {
                return statsResponse
            }

            val message = getFailedEvaluateMessage(indexName)
            logger.warn("$message - ${statsResponse.status}")
            stepStatus = StepStatus.FAILED
            info = mapOf(
                "message" to message,
                "shard_failures" to statsResponse.shardFailures.map { it.getUsefulCauseString() }
            )
        } catch (e: RemoteTransportException) {
            handleException(ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(e, getFailedEvaluateMessage(indexName))
        }

        return null
    }

    private fun handleException(e: Exception, message: String = getFailedMessage(indexName)) {
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
            rolledOver = if (currentMetaData.rolledOver == true) true else stepStatus == StepStatus.COMPLETED,
            transitionTo = null,
            info = info
        )
    }

    companion object {
        fun getFailedMessage(index: String) = "Failed to rollover index [index=$index]"
        fun getFailedAliasUpdateMessage(index: String, newIndex: String) =
            "New index created, but failed to update alias [index=$index, newIndex=$newIndex]"
        fun getFailedDataStreamRolloverMessage(dataStream: String) = "Failed to rollover data stream [data_stream=$dataStream]"
        fun getFailedNoValidAliasMessage(index: String) = "Missing rollover_alias index setting [index=$index]"
        fun getFailedDuplicateRolloverMessage(index: String) = "Index has already been rolled over [index=$index]"
        fun getFailedEvaluateMessage(index: String) = "Failed to evaluate conditions for rollover [index=$index]"
        fun getPendingMessage(index: String) = "Pending rollover of index [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully rolled over index [index=$index]"
        fun getSuccessDataStreamRolloverMessage(dataStream: String, index: String) =
            "Successfully rolled over data stream [data_stream=$dataStream index=$index]"
    }
}
