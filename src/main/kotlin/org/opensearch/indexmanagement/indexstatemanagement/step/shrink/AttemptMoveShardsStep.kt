/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.Tuple
import org.opensearch.common.settings.Settings
import org.opensearch.index.shard.DocsStats
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.util.getActionStartTime
import org.opensearch.indexmanagement.indexstatemanagement.util.getFreeBytesThresholdHigh
import org.opensearch.indexmanagement.indexstatemanagement.util.isIndexGreen
import org.opensearch.indexmanagement.indexstatemanagement.util.issueUpdateSettingsRequest
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.jobscheduler.repackage.com.cronutils.utils.VisibleForTesting
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.LockModel
import java.lang.Exception
import java.time.Duration
import java.time.Instant
import java.util.PriorityQueue
import kotlin.collections.ArrayList
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.min
import kotlin.math.sqrt

@SuppressWarnings("TooManyFunctions")
class AttemptMoveShardsStep(private val action: ShrinkAction) : Step(name) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var shrinkActionProperties: ShrinkActionProperties? = null

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount", "LongMethod")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val client = context.client
        val indexName = context.metadata.index
        try {
            if (actionTimedOut(context.metadata)) return this

            val shrinkTargetIndexName = indexName + (action.targetIndexSuffix ?: DEFAULT_TARGET_SUFFIX)
            if (targetIndexNameExists(context.clusterService, shrinkTargetIndexName)) return this

            if (!isIndexGreen(client, indexName)) {
                info = mapOf("message" to FAILURE_MESSAGE)
                stepStatus = StepStatus.CONDITION_NOT_MET
                return this
            }

            if (shouldFailUnsafe(context.clusterService, indexName)) return this

            // Fail if there is only one primary shard, as that cannot be shrunk
            val numOriginalShards = context.clusterService.state().metadata.indices[indexName].numberOfShards
            //
            if (numOriginalShards == 1) {
                info = mapOf("message" to ONE_PRIMARY_SHARD_MESSAGE)
                stepStatus = StepStatus.COMPLETED
                return this
            }

            // Get the size of the index
            val statsRequest = IndicesStatsRequest().indices(indexName)
            val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil {
                stats(statsRequest, it)
            }
            val statsStore = statsResponse.total.store
            val statsDocs = statsResponse.total.docs
            if (statsStore == null || statsDocs == null) {
                info = mapOf("message" to FAILURE_MESSAGE)
                stepStatus = StepStatus.FAILED
                return this
            }
            val indexSize = statsStore.sizeInBytes
            val numTargetShards = getNumTargetShards(numOriginalShards, indexSize)

            if (shouldFailTooManyDocuments(statsDocs, numTargetShards)) return this

            // get the nodes with enough memory in increasing order of free space
            val suitableNodes = findSuitableNodes(context, statsResponse, indexSize)
            // iterate through the nodes and try to acquire a lock on one
            val lock = acquireLockOnNode(context.jobContext, suitableNodes)
            if (lock == null) {
                logger.info("$indexName could not find available node to shrink onto.")
                info = mapOf("message" to NO_AVAILABLE_NODES_MESSAGE)
                stepStatus = StepStatus.CONDITION_NOT_MET
                return this
            }
            val nodeName = lock.resource[RESOURCE_NAME] as String
            shrinkActionProperties = ShrinkActionProperties(
                nodeName,
                shrinkTargetIndexName,
                numTargetShards,
                lock.primaryTerm,
                lock.seqNo,
                lock.lockTime.epochSecond
            )
            setToReadOnlyAndMoveIndexToNode(context, nodeName, lock)
            info = mapOf("message" to getSuccessMessage(nodeName))
            stepStatus = StepStatus.COMPLETED
            return this
        } catch (e: Exception) {
            info = mapOf("message" to FAILURE_MESSAGE, "cause" to "{${e.message}}")
            stepStatus = StepStatus.FAILED
            return this
        }
    }

    private fun shouldFailTooManyDocuments(docsStats: DocsStats, numTargetShards: Int): Boolean {
        val totalDocs: Long = docsStats.count
        val docsPerTargetShard: Long = totalDocs / numTargetShards
        // The maximum number of documents per shard is 2^31
        val maximumDocsPerShard = 0x80000000
        if (docsPerTargetShard > maximumDocsPerShard) {
            info = mapOf("message" to TOO_MANY_DOCS_FAILURE_MESSAGE)
            stepStatus = StepStatus.FAILED
            return true
        }
        return false
    }

    /*
     * Returns whether the action should fail due to being unsafe. The action is unsafe if there are no replicas. If forceUnsafe
     * is set, then this always returns false.
     */
    private fun shouldFailUnsafe(clusterService: ClusterService, indexName: String): Boolean {
        // If forceUnsafe is set and is true, then we don't even need to check the number of replicas
        if (action.forceUnsafe == true) return false
        val numReplicas = clusterService.state().metadata.indices[indexName].numberOfReplicas
        val shouldFailForceUnsafeCheck = numReplicas == 0
        if (shouldFailForceUnsafeCheck) {
            info = mapOf("message" to UNSAFE_FAILURE_MESSAGE)
            stepStatus = StepStatus.FAILED
            return true
        }
        return false
    }

    private fun targetIndexNameExists(clusterService: ClusterService, shrinkTargetIndexName: String): Boolean {
        val indexExists = clusterService.state().metadata.indices.containsKey(shrinkTargetIndexName)
        if (indexExists) {
            info = mapOf("message" to getIndexExistsMessage(shrinkTargetIndexName))
            stepStatus = StepStatus.FAILED
            return true
        }
        return false
    }

    private suspend fun setToReadOnlyAndMoveIndexToNode(stepContext: StepContext, node: String, lock: LockModel) {
        val updateSettings = Settings.builder()
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
            .put(ROUTING_SETTING, node)
            .build()
        val jobContext = stepContext.jobContext
        try {
            val response: AcknowledgedResponse = issueUpdateSettingsRequest(stepContext.client, stepContext.metadata.index, updateSettings)
            if (!response.isAcknowledged) {
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to UPDATE_FAILED_MESSAGE)
                jobContext.lockService.suspendUntil<Boolean> { release(lock, it) }
            }
        } catch (e: Exception) {
            stepStatus = StepStatus.FAILED
            handleException(e, UPDATE_FAILED_MESSAGE)
            jobContext.lockService.suspendUntil<Boolean> { release(lock, it) }
        }
    }

    /*
     * Iterates through each suitable node in order, attempting to acquire a resource lock. Returns the first lock which
     * is successfully acquired.
     */
    private suspend fun acquireLockOnNode(jobContext: JobExecutionContext, suitableNodes: List<String>): LockModel? {
        for (node in suitableNodes) {
            val nodeResourceObject = mapOf(RESOURCE_NAME to node)
            // TODO CLAY, the lock should be the timeout for all steps, not just one??
            val lockTime = action.configTimeout?.timeout?.seconds ?: MOVE_SHARDS_TIMEOUT_IN_SECONDS
            val lock: LockModel? = jobContext.lockService.suspendUntil {
                acquireLockOnResource(jobContext, lockTime, RESOURCE_TYPE, nodeResourceObject, it)
            }
            if (lock != null) {
                return lock
            }
        }
        return null
    }

    /*
     * Returns the list of node names for nodes with enough space to shrink to, in increasing order of space available
     */
    @VisibleForTesting
    @SuppressWarnings("NestedBlockDepth", "ComplexMethod")
    private suspend fun findSuitableNodes(
        stepContext: StepContext,
        indicesStatsResponse: IndicesStatsResponse,
        indexSizeInBytes: Long
    ): List<String> {
        val nodesStatsReq = NodesStatsRequest().addMetric(OS_METRIC)
        val nodeStatsResponse: NodesStatsResponse = stepContext.client.admin().cluster().suspendUntil { nodesStats(nodesStatsReq, it) }
        val nodesList = nodeStatsResponse.nodes
        // Sort in increasing order of keys, in our case this is memory left
        val comparator = kotlin.Comparator { o1: Tuple<Long, String>, o2: Tuple<Long, String> -> o1.v1().compareTo(o2.v1()) }
        val nodesWithSpace = PriorityQueue(comparator)
        for (node in nodesList) {
            val osStats = node.os
            if (osStats != null) {
                val memLeftInNode = osStats.mem.free.bytes
                val totalNodeMem = osStats.mem.total.bytes
                val freeBytesThresholdHigh = getFreeBytesThresholdHigh(stepContext.settings, stepContext.clusterService.clusterSettings, totalNodeMem)
                // We require that a node has enough space to be below the high watermark disk level with an additional 2 * the index size free
                val requiredBytes = (2 * indexSizeInBytes) + freeBytesThresholdHigh
                if (memLeftInNode > requiredBytes) {
                    val memLeftAfterTransfer: Long = memLeftInNode - requiredBytes
                    nodesWithSpace.add(Tuple(memLeftAfterTransfer, node.node.name))
                }
            }
        }
        val suitableNodes: ArrayList<String> = ArrayList()
        // For each node, do a dry run of moving all shards to the node to make sure there is enough space.
        // This should be rejected if allocation puts it above the low disk watermark setting
        for (sizeNodeTuple in nodesWithSpace) {
            val nodeName = sizeNodeTuple.v2()
            val indexName = stepContext.metadata.index
            val clusterRerouteRequest = ClusterRerouteRequest().explain(true).dryRun(true)
            for (shard in indicesStatsResponse.shards) {
                val shardId = shard.shardRouting.shardId()
                val currentShardNode = stepContext.clusterService.state().nodes[shard.shardRouting.currentNodeId()]
                clusterRerouteRequest.add(MoveAllocationCommand(indexName, shardId.id, currentShardNode.name, nodeName))
            }
            val clusterRerouteResponse: ClusterRerouteResponse =
                stepContext.client.admin().cluster().suspendUntil { reroute(clusterRerouteRequest, it) }
            // Should be the same number of yes decisions as the number of primary shards
            if (clusterRerouteResponse.explanations.yesDecisionMessages.size == indicesStatsResponse.shards.size) {
                suitableNodes.add(sizeNodeTuple.v2())
            }
        }
        return suitableNodes
    }

    @SuppressWarnings("ReturnCount")
    private fun getNumTargetShards(numOriginalShards: Int, indexSize: Long): Int {
        // case where user specifies a certain number of shards in the target index
        if (action.numNewShards != null) return getGreatestFactorLessThan(numOriginalShards, action.numNewShards)

        // case where user specifies a percentage of source shards to shrink to in the number of shards in the target index
        if (action.percentageOfSourceShards != null) {
            val numTargetShards = floor((action.percentageOfSourceShards) * numOriginalShards).toInt()
            return getGreatestFactorLessThan(numOriginalShards, numTargetShards)
        }
        // case where the user specifies a max shard size in the target index
        val maxShardSizeInBytes = action.maxShardSize!!.bytes
        // ceiling ensures that numTargetShards is never less than 1
        val minNumTargetShards = ceil(indexSize / maxShardSizeInBytes.toDouble()).toInt()
        // In order to not violate the max shard size condition, this value must be >= minNumTargetShards.
        // If that value doesn't exist, numOriginalShards will be returned
        return getMinFactorGreaterThan(numOriginalShards, minNumTargetShards)
    }

    /*
     * Returns the greatest number which is <= k and is a factor of n. In the context of the shrink action,
     * n is the original number of shards, k is the attempted number of shards to shrink to. If k is 0, 1 is returned.
     */
    @SuppressWarnings("ReturnCount")
    private fun getGreatestFactorLessThan(n: Int, k: Int): Int {
        if (k >= n) return n
        // The bound is set to the floor of the square root of n, or just k, whichever is lower
        val bound: Int = min(floor(sqrt(n.toDouble())).toInt(), k)
        var greatestFactor = 1
        for (i in 2..bound) {
            if (n % i == 0) {
                val complement: Int = n / i
                if (complement <= k) {
                    return complement
                } else {
                    greatestFactor = i
                }
            }
        }
        return greatestFactor
    }

    /*
     * Returns the smallest number which is >= k and is a factor of n. In the context of the shrink action,
     * n is the original number of shards, k is the attempted number of shards to shrink to in a case
     */
    @SuppressWarnings("ReturnCount")
    private fun getMinFactorGreaterThan(n: Int, k: Int): Int {
        if (k >= n) {
            return n
        }
        for (i in k..n) {
            if (n % i == 0) return i
        }
        return n
    }

    private fun handleException(e: Exception, message: String) {
        logger.error(message, e)
        stepStatus = StepStatus.FAILED
        val mutableInfo = mutableMapOf("message" to message)
        val errorMessage = e.message
        if (errorMessage != null) mutableInfo["cause"] = errorMessage
        info = mutableInfo.toMap()
    }

    private fun actionTimedOut(managedIndexMetadata: ManagedIndexMetaData): Boolean {
        val timeFromActionStarted: Duration = Duration.between(getActionStartTime(managedIndexMetadata), Instant.now())
        val timeOutInSeconds = action.configTimeout?.timeout?.seconds ?: MOVE_SHARDS_TIMEOUT_IN_SECONDS
        // Get ActionTimeout if given, otherwise use default timeout of 12 hours
        if (timeFromActionStarted.toSeconds() > timeOutInSeconds) {
            info = mapOf("message" to TIMEOUT_MESSAGE)
            stepStatus = StepStatus.FAILED
            return true
        }
        return false
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetadata.actionMetaData
        // If we succeeded because there was only one source primary shard, we no-op by skipping to the last step
        val stepMetaData = if (info?.get("message") == ONE_PRIMARY_SHARD_MESSAGE) {
            StepMetaData(WaitForShrinkStep.name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus)
        } else {
            StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus)
        }
        return currentMetadata.copy(
            actionMetaData = currentActionMetaData?.copy(
                actionProperties = ActionProperties(
                    shrinkActionProperties = shrinkActionProperties
                )
            ),
            stepMetaData = stepMetaData,
            transitionTo = null,
            info = info
        )
    }

    override fun isIdempotent() = true

    companion object {
        const val OS_METRIC = "os"
        const val ROUTING_SETTING = "index.routing.allocation.require._name"
        const val RESOURCE_NAME = "node_name"
        const val DEFAULT_TARGET_SUFFIX = "_shrunken"
        const val MOVE_SHARDS_TIMEOUT_IN_SECONDS = 43200L // 12hrs in seconds
        const val name = "attempt_move_shards_step"
        const val RESOURCE_TYPE = "shrink"
        const val TIMEOUT_MESSAGE = "Timed out waiting for finding node."
        const val UPDATE_FAILED_MESSAGE = "Shrink failed because settings could not be updated.."
        const val NO_AVAILABLE_NODES_MESSAGE =
            "There are no available nodes for to move to to execute a shrink. Delaying until node becomes available."
        const val UNSAFE_FAILURE_MESSAGE = "Shrink failed because index has no replicas and force_unsafe is not set to true."
        const val ONE_PRIMARY_SHARD_MESSAGE = "Shrink action did not do anything because source index only has one primary shard."
        const val TOO_MANY_DOCS_FAILURE_MESSAGE = "Shrink failed because there would be too many documents on each target shard following the shrink."
        const val FAILURE_MESSAGE = "Shrink failed to start moving shards."
        fun getSuccessMessage(node: String) = "Successfully started moving the shards to $node."
        fun getIndexExistsMessage(newIndex: String) = "Shrink failed because $newIndex already exists."
    }
}
