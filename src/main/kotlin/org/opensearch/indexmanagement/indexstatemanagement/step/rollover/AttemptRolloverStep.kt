/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.rollover

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.opensearch.action.admin.indices.rollover.RolloverRequest
import org.opensearch.action.admin.indices.rollover.RolloverResponse
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getRolloverAlias
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getRolloverSkip
import org.opensearch.indexmanagement.indexstatemanagement.util.evaluateConditions
import org.opensearch.indexmanagement.opensearchapi.getUsefulCauseString
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.rest.RestStatus
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

@Suppress("ReturnCount")
class AttemptRolloverStep(private val action: RolloverAction) : Step(name) {

    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var newIndex: String? = null // this variable holds the new index name if rollover is successful in this run

    @Suppress("ComplexMethod", "LongMethod")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index
        val clusterService = context.clusterService
        val skipRollover = clusterService.state().metadata.index(indexName).getRolloverSkip()
        if (skipRollover) {
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getSkipRolloverMessage(indexName))
            return this
        }

        val (rolloverTarget, isDataStream) = getRolloverTargetOrUpdateInfo(context)
        // If the rolloverTarget is null, we would've already updated the failed info from getRolloverTargetOrUpdateInfo and can return early
        rolloverTarget ?: return this

        if (clusterService.state().metadata.index(indexName).rolloverInfos.containsKey(rolloverTarget)) {
            stepStatus = StepStatus.COMPLETED
            info = mapOf("message" to getAlreadyRolledOverMessage(indexName, rolloverTarget))

            // If already rolled over, alias may not get copied over yet
            copyAlias(clusterService, indexName, context.client, rolloverTarget, context.metadata)
            return this
        }

        if (!isDataStream && !preCheckIndexAlias(context, rolloverTarget)) {
            stepStatus = StepStatus.FAILED
            info = mapOf("message" to getFailedPreCheckMessage(indexName))
            return this
        }

        val statsResponse = getIndexStatsOrUpdateInfo(context)
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
        val largestPrimaryShard = statsResponse.shards.maxByOrNull { it.stats.docs?.totalSizeInBytes ?: 0 }
        val largestPrimaryShardSize = ByteSizeValue(largestPrimaryShard?.stats?.docs?.totalSizeInBytes ?: 0)

        val conditions = listOfNotNull(
            action.minAge?.let {
                RolloverAction.MIN_INDEX_AGE_FIELD to mapOf(
                    "condition" to it.toString(),
                    "current" to indexAgeTimeValue.toString(),
                    "creationDate" to indexCreationDate
                )
            },
            action.minDocs?.let {
                RolloverAction.MIN_DOC_COUNT_FIELD to mapOf(
                    "condition" to it,
                    "current" to numDocs
                )
            },
            action.minSize?.let {
                RolloverAction.MIN_SIZE_FIELD to mapOf(
                    "condition" to it.toString(),
                    "current" to indexSize.toString()
                )
            },
            action.minPrimaryShardSize?.let {
                RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD to mapOf(
                    "condition" to it.toString(),
                    "current" to largestPrimaryShardSize.toString(),
                    "shard" to largestPrimaryShard?.shardRouting?.id()
                )
            }
        ).toMap()

        if (action.evaluateConditions(indexAgeTimeValue, numDocs, indexSize, largestPrimaryShardSize)) {
            logger.info(
                "$indexName rollover conditions evaluated to true [indexCreationDate=$indexCreationDate," +
                    " numDocs=$numDocs, indexSize=${indexSize.bytes}, primaryShardSize=${largestPrimaryShardSize.bytes}]"
            )
            executeRollover(context, rolloverTarget, isDataStream, conditions)
            copyAlias(clusterService, indexName, context.client, rolloverTarget, context.metadata)
        } else {
            stepStatus = StepStatus.CONDITION_NOT_MET
            info = mapOf("message" to getPendingMessage(indexName), "conditions" to conditions)
        }

        return this
    }

    private fun getRolloverTargetOrUpdateInfo(context: StepContext): Pair<String?, Boolean> {
        val indexName = context.metadata.index
        val metadata = context.clusterService.state().metadata()
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

    /**
     * pre-condition check on managed-index's alias before rollover
     *
     * This will block
     *  when managed index doesn't have alias
     *  when managed index has alias but not the write index,
     *      and this alias contains more than one index
     * User can use skip rollover setting to bypass this
     *
     * @param alias user defined ISM rollover alias
     */
    private fun preCheckIndexAlias(context: StepContext, alias: String): Boolean {
        val indexName = context.metadata.index
        val metadata = context.clusterService.state().metadata
        val indexAlias = metadata.index(indexName)?.aliases?.get(alias)
        logger.debug("Index {} has aliases {}", indexName, indexAlias)
        if (indexAlias == null) {
            return false
        }
        val isWriteIndex = indexAlias.writeIndex() // this could be null
        if (isWriteIndex != true) {
            val aliasIndices = metadata.indicesLookup[alias]?.indices?.map { it.index }
            logger.debug("Alias {} contains indices {}", alias, aliasIndices)
            if (aliasIndices != null && aliasIndices.size > 1) {
                return false
            }
        }

        return true
    }

    private suspend fun getIndexStatsOrUpdateInfo(context: StepContext): IndicesStatsResponse? {
        val indexName = context.metadata.index
        try {
            val statsRequest = IndicesStatsRequest()
                .indices(indexName).clear().docs(true)
            val statsResponse: IndicesStatsResponse = context.client.admin().indices().suspendUntil { stats(statsRequest, it) }

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
            handleException(indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(indexName, e, getFailedEvaluateMessage(indexName))
        }

        return null
    }

    @Suppress("ComplexMethod")
    private suspend fun executeRollover(
        context: StepContext,
        rolloverTarget: String,
        isDataStream: Boolean,
        conditions: Map<String, Map<String, Any?>>
    ) {
        val indexName = context.metadata.index
        try {
            val request = RolloverRequest(rolloverTarget, null)
            val response: RolloverResponse = context.client.admin().indices().suspendUntil { rolloverIndex(request, it) }

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

                // Save newIndex later to metadata to be reused in case of failures
                newIndex = response.newIndex

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
            handleException(indexName, ExceptionsHelper.unwrapCause(e) as Exception)
        } catch (e: Exception) {
            handleException(indexName, e)
        }
    }

    /**
     * This method should be called ASAP after rollover succeed
     *
     * Rollover currently only copy the alias being rolled over on to new index
     * This method copy any remaining aliases to new index
     *
     * TODO This method can be deprecated once this issue finished
     * https://github.com/opensearch-project/index-management/issues/849 finished
     */
    @Suppress("ComplexMethod")
    private suspend fun copyAlias(
        clusterService: ClusterService,
        indexName: String,
        client: Client,
        rolloverTarget: String,
        metadata: ManagedIndexMetaData
    ) {
        if (!action.copyAlias) return

        // Try to preserve the rollover conditions
        val conditions = info?.get("conditions") ?: context?.metadata?.info?.get("conditions")

        val rolledOverIndexName = newIndex ?: metadata.rolledOverIndexName
        if (rolledOverIndexName == null) {
            // Only in rare case when the program shut down unexpectedly before rolledOverIndexName is set or metadata corrupted
            // ISM cannot auto recover from this case, so the status is COMPLETED
            logger.error("$indexName rolled over but cannot find the rolledOverIndexName to copy aliases to")
            stepStatus = StepStatus.COMPLETED
            info = listOfNotNull(
                "message" to getCopyAliasRolledOverIndexNotFoundMessage(indexName),
                if (conditions != null) "conditions" to conditions else null
            ).toMap()
            return
        }

        val aliasActions = mutableListOf<AliasActions>()
        val aliases = clusterService.state().metadata().index(indexName).aliases
        for (alias in aliases) {
            val aliasName = alias.key
            // Skip the alias that has been rolled over on, since it's already copied
            if (aliasName == rolloverTarget) continue

            val aliasMetadata = alias.value
            val aliasAction = AliasActions(AliasActions.Type.ADD).index(rolledOverIndexName)
                .alias(aliasMetadata.alias)
                .filter(aliasMetadata.filter?.toString())
                .searchRouting(aliasMetadata.searchRouting)
                .indexRouting(aliasMetadata.indexRouting)
                .isHidden(aliasMetadata.isHidden)
            aliasActions.add(aliasAction)
        }
        val aliasReq = IndicesAliasesRequest()
        aliasActions.forEach { aliasReq.addAliasAction(it) }

        try {
            val aliasRes: AcknowledgedResponse = client.admin().indices().suspendUntil { aliases(aliasReq, it) }
            if (aliasRes.isAcknowledged) {
                stepStatus = StepStatus.COMPLETED
                info = listOfNotNull(
                    "message" to getSuccessCopyAliasMessage(indexName, rolledOverIndexName),
                    if (conditions != null) "conditions" to conditions else null
                ).toMap()
            } else {
                stepStatus = StepStatus.FAILED
                info = listOfNotNull(
                    "message" to getFailedCopyAliasMessage(indexName, rolledOverIndexName),
                    if (conditions != null) "conditions" to conditions else null
                ).toMap()
            }
        } catch (e: IndexNotFoundException) {
            logger.error("Index not found while copying alias from $indexName to $rolledOverIndexName", e)
            stepStatus = StepStatus.FAILED
            info = listOfNotNull(
                "message" to getCopyAliasIndexNotFoundMessage(rolledOverIndexName),
                if (conditions != null) "conditions" to conditions else null
            ).toMap()
        } catch (e: Exception) {
            handleException(indexName, e, getFailedCopyAliasMessage(indexName, rolledOverIndexName), conditions)
        }
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        return currentMetadata.copy(
            stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
            rolledOver = if (currentMetadata.rolledOver == true) true else stepStatus == StepStatus.COMPLETED,
            rolledOverIndexName = if (currentMetadata.rolledOverIndexName != null) currentMetadata.rolledOverIndexName else newIndex,
            transitionTo = null,
            info = info
        )
    }

    private fun handleException(indexName: String, e: Exception, message: String = getFailedMessage(indexName), conditions: Any? = null) {
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo: MutableMap<String, Any> = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        if (conditions != null) mutableInfo["conditions"] = conditions
        info = mutableInfo.toMap()
    }

    override fun isIdempotent(): Boolean = false

    @Suppress("TooManyFunctions")
    companion object {
        const val name = "attempt_rollover"
        fun getFailedMessage(index: String) = "Failed to rollover index [index=$index]"
        fun getFailedAliasUpdateMessage(index: String, newIndex: String) =
            "New index created, but failed to update alias [index=$index, newIndex=$newIndex]"
        fun getFailedDataStreamRolloverMessage(dataStream: String) = "Failed to rollover data stream [data_stream=$dataStream]"
        fun getFailedNoValidAliasMessage(index: String) = "Missing rollover_alias index setting [index=$index]"
        fun getFailedEvaluateMessage(index: String) = "Failed to evaluate conditions for rollover [index=$index]"
        fun getPendingMessage(index: String) = "Pending rollover of index [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully rolled over index [index=$index]"
        fun getSuccessDataStreamRolloverMessage(dataStream: String, index: String) =
            "Successfully rolled over data stream [data_stream=$dataStream index=$index]"
        fun getFailedPreCheckMessage(index: String) = "Missing alias or not the write index when rollover [index=$index]"
        fun getSkipRolloverMessage(index: String) = "Skipped rollover action for [index=$index]"
        fun getAlreadyRolledOverMessage(index: String, alias: String) =
            "This index has already been rolled over using this alias, treating as a success [index=$index, alias=$alias]"
        fun getSuccessCopyAliasMessage(index: String, newIndex: String) =
            "Successfully rolled over and copied alias from [index=$index] to [index=$newIndex]"
        fun getFailedCopyAliasMessage(index: String, newIndex: String) =
            "Successfully rolled over but failed to copied alias from [index=$index] to [index=$newIndex]"
        fun getCopyAliasIndexNotFoundMessage(newIndex: String?) =
            "Successfully rolled over but new index [index=$newIndex] not found during copy alias"
        fun getCopyAliasRolledOverIndexNotFoundMessage(index: String?) =
            "Successfully rolled over [index=$index] but ISM cannot find rolled over index from metadata to copy aliases to, please manually copy"
    }
}
