/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchSecurityException
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand
import org.opensearch.cluster.routing.allocation.decider.Decision
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.Tuple
import org.opensearch.common.settings.Settings
import org.opensearch.index.shard.DocsStats
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.util.getIntervalFromManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.util.getManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.util.getNodeFreeMemoryAfterShrink
import org.opensearch.indexmanagement.indexstatemanagement.util.getShrinkLockID
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
import java.util.PriorityQueue
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
            val shrinkTargetIndexName = indexName + (action.targetIndexSuffix ?: DEFAULT_TARGET_SUFFIX)
            if (targetIndexNameExists(context.clusterService, shrinkTargetIndexName)) return this

            if (!isIndexGreen(client, indexName)) {
                info = mapOf("message" to INDEX_NOT_GREEN_MESSAGE)
                stepStatus = StepStatus.CONDITION_NOT_MET
                return this
            }

            if (shouldFailUnsafe(context.clusterService, indexName)) return this

            // Fail if there is only one primary shard, as that cannot be shrunk
            val numOriginalShards = context.clusterService.state().metadata.indices[indexName].numberOfShards
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
                logger.error("Failed to move shards in shrink action as IndicesStatsResponse was missing store or doc stats.")
                fail(FAILURE_MESSAGE)
                return this
            }
            val indexSize = statsStore.sizeInBytes
            val numTargetShards = getNumTargetShards(numOriginalShards, indexSize)

            if (shouldFailTooManyDocuments(statsDocs, numTargetShards)) return this

            // get the nodes with enough memory in increasing order of free space
            val suitableNodes = findSuitableNodes(context, statsResponse, indexSize)

            // Get the job interval to use in determining the lock length
            val interval = getJobIntervalSeconds(context.metadata.indexUuid, client)
            // iterate through the nodes and try to acquire a lock on one
            val lockToNodeName: Pair<LockModel, String>? = acquireLockFromNodeList(context.jobContext, suitableNodes, interval)
            if (lockToNodeName == null) {
                logger.info("$indexName could not find available node to shrink onto.")
                info = mapOf("message" to NO_AVAILABLE_NODES_MESSAGE)
                stepStatus = StepStatus.CONDITION_NOT_MET
                return this
            }
            val (lock, nodeName) = lockToNodeName
            shrinkActionProperties = ShrinkActionProperties(
                nodeName,
                shrinkTargetIndexName,
                numTargetShards,
                lock.primaryTerm,
                lock.seqNo,
                lock.lockTime.epochSecond,
                lock.lockDurationSeconds
            )

            setToReadOnlyAndMoveIndexToNode(context, nodeName, lock)
            info = mapOf("message" to getSuccessMessage(nodeName))
            stepStatus = StepStatus.COMPLETED
            return this
        } catch (e: OpenSearchSecurityException) {
            fail(getSecurityFailureMessage(e.localizedMessage), e.message, e)
            return this
        } catch (e: Exception) {
            fail(FAILURE_MESSAGE, e.message, e)
            return this
        }
    }

    private fun fail(message: String, cause: String? = null, e: Exception? = null) {
        e?.let { logger.error(message, e) }
        info = if (cause == null) mapOf("message" to message) else mapOf("message" to message, "cause" to cause)
        stepStatus = StepStatus.FAILED
        shrinkActionProperties = null
    }

    private suspend fun getJobIntervalSeconds(indexUuid: String, client: Client): Long? {
        val managedIndexConfig: ManagedIndexConfig?
        try {
            managedIndexConfig = getManagedIndexConfig(indexUuid, client)
        } catch (e: Exception) {
            // If we fail to get the managedIndexConfig, just return null and a default lock duration of 12 hours will be used later
            return null
        }
        // Divide the interval by 1000 to convert from ms to seconds
        return managedIndexConfig?.let { getIntervalFromManagedIndexConfig(it) / MILLISECONDS_IN_SECOND }
    }

    private fun shouldFailTooManyDocuments(docsStats: DocsStats, numTargetShards: Int): Boolean {
        val totalDocs: Long = docsStats.count
        val docsPerTargetShard: Long = totalDocs / numTargetShards
        if (docsPerTargetShard > MAXIMUM_DOCS_PER_SHARD) {
            logger.error(TOO_MANY_DOCS_FAILURE_MESSAGE)
            fail(TOO_MANY_DOCS_FAILURE_MESSAGE)
            return true
        }
        return false
    }

    /*
     * Returns whether the action should fail due to being unsafe. The action is unsafe if there are no replicas. If forceUnsafe
     * is set, then this always returns false.
     */
    @Suppress("ReturnCount")
    private fun shouldFailUnsafe(clusterService: ClusterService, indexName: String): Boolean {
        // If forceUnsafe is set and is true, then we don't even need to check the number of replicas
        if (action.forceUnsafe == true) return false
        val numReplicas = clusterService.state().metadata.indices[indexName].numberOfReplicas
        val shouldFailForceUnsafeCheck = numReplicas == 0
        if (shouldFailForceUnsafeCheck) {
            logger.error(UNSAFE_FAILURE_MESSAGE)
            fail(UNSAFE_FAILURE_MESSAGE)
            return true
        }
        return false
    }

    private fun targetIndexNameExists(clusterService: ClusterService, shrinkTargetIndexName: String): Boolean {
        val indexExists = clusterService.state().metadata.indices.containsKey(shrinkTargetIndexName)
        if (indexExists) {
            val indexExistsMessage = getIndexExistsMessage(shrinkTargetIndexName)
            logger.error(indexExistsMessage)
            fail(indexExistsMessage)
            return true
        }
        return false
    }

    private suspend fun setToReadOnlyAndMoveIndexToNode(stepContext: StepContext, node: String, lock: LockModel): Boolean {
        val updateSettings = Settings.builder()
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
            .put(ROUTING_SETTING, node)
            .build()
        val jobContext = stepContext.jobContext
        var response: AcknowledgedResponse? = null
        val isUpdateAcknowledged: Boolean
        try {
            response = issueUpdateSettingsRequest(stepContext.client, stepContext.metadata.index, updateSettings)
        } finally {
            isUpdateAcknowledged = response != null && response.isAcknowledged
            if (!isUpdateAcknowledged) {
                fail(UPDATE_FAILED_MESSAGE)
                val released: Boolean = jobContext.lockService.suspendUntil { release(lock, it) }
                if (!released) {
                    logger.error("Failed to release Shrink action lock on node [$node]")
                }
            }
        }
        return isUpdateAcknowledged
    }

    /*
     * Iterates through each suitable node in order, attempting to acquire a resource lock. Returns the first lock which
     * is successfully acquired and the name of the node it acquired the lock on in a pair.
     */
    private suspend fun acquireLockFromNodeList(
        jobContext: JobExecutionContext,
        suitableNodes: List<String>,
        jobIntervalSeconds: Long?
    ): Pair<LockModel, String>? {
        for (nodeName in suitableNodes) {
            val lockID = getShrinkLockID(nodeName)
            val lock: LockModel? = jobContext.lockService.suspendUntil {
                acquireLockWithId(jobContext.jobIndexName, getShrinkLockDuration(jobIntervalSeconds), lockID, it)
            }
            if (lock != null) {
                return lock to nodeName
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
            val remainingMem = getNodeFreeMemoryAfterShrink(node, indexSizeInBytes, stepContext.settings, stepContext.clusterService.clusterSettings)
            if (remainingMem > 0L) {
                nodesWithSpace.add(Tuple(remainingMem, node.node.name))
            }
        }
        val suitableNodes: ArrayList<String> = ArrayList()
        // For each node, do a dry run of moving all shards to the node to make sure there is enough space.
        // This should be rejected if allocation puts it above the low disk watermark setting
        for (sizeNodeTuple in nodesWithSpace) {
            val targetNodeName = sizeNodeTuple.v2()
            val indexName = stepContext.metadata.index
            val clusterRerouteRequest = ClusterRerouteRequest().explain(true).dryRun(true)
            var numberOfRerouteRequests = 0
            for (shard in indicesStatsResponse.shards) {
                val shardId = shard.shardRouting.shardId()
                val currentShardNode = stepContext.clusterService.state().nodes[shard.shardRouting.currentNodeId()]
                // Don't attempt a dry run for shards which are already on that node
                if (currentShardNode.name == targetNodeName) continue
                clusterRerouteRequest.add(MoveAllocationCommand(indexName, shardId.id, currentShardNode.name, targetNodeName))
                numberOfRerouteRequests++
            }
            val clusterRerouteResponse: ClusterRerouteResponse =
                stepContext.client.admin().cluster().suspendUntil { reroute(clusterRerouteRequest, it) }
            val numYesDecisions = clusterRerouteResponse.explanations.explanations().count { it.decisions().type().equals((Decision.Type.YES)) }
            // Should be the same number of yes decisions as the number of primary shards
            if (numYesDecisions == numberOfRerouteRequests) {
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
        const val DEFAULT_TARGET_SUFFIX = "_shrunken"
        const val name = "attempt_move_shards_step"
        const val UPDATE_FAILED_MESSAGE = "Shrink failed because shard settings could not be updated."
        const val NO_AVAILABLE_NODES_MESSAGE =
            "There are no available nodes for to move to to execute a shrink. Delaying until node becomes available."
        const val UNSAFE_FAILURE_MESSAGE = "Shrink failed because index has no replicas and force_unsafe is not set to true."
        const val ONE_PRIMARY_SHARD_MESSAGE = "Shrink action did not do anything because source index only has one primary shard."
        const val TOO_MANY_DOCS_FAILURE_MESSAGE = "Shrink failed because there would be too many documents on each target shard following the shrink."
        const val INDEX_NOT_GREEN_MESSAGE = "Shrink action cannot start moving shards as the index is not green."
        const val FAILURE_MESSAGE = "Shrink failed to start moving shards."
        private const val DEFAULT_LOCK_INTERVAL = 3L * 60L * 60L // Default lock interval is 3 hours in seconds
        private const val MILLISECONDS_IN_SECOND = 1000
        private const val JOB_INTERVAL_LOCK_MULTIPLIER = 3
        private const val LOCK_BUFFER_SECONDS = 1800
        private const val MAXIMUM_DOCS_PER_SHARD = 0x80000000 // The maximum number of documents per shard is 2^31
        fun getSuccessMessage(node: String) = "Successfully started moving the shards to $node."
        fun getIndexExistsMessage(newIndex: String) = "Shrink failed because $newIndex already exists."
        fun getSecurityFailureMessage(failure: String) = "Shrink action failed because of missing permissions: $failure"
        // If we couldn't get the job interval for the lock, use the default of 12 hours.
        // Lock is 3x + 30 minutes the job interval to allow the next step's execution to extend the lock without losing it.
        // If user sets maximum jitter, it could be 2x the job interval before the next step is executed.
        private fun getShrinkLockDuration(jobInterval: Long?) = jobInterval?.let { (it * JOB_INTERVAL_LOCK_MULTIPLIER) + LOCK_BUFFER_SECONDS }
            ?: DEFAULT_LOCK_INTERVAL
    }
}
