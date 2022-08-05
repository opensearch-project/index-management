/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.shrink

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE
import org.opensearch.cluster.metadata.MetadataCreateIndexService.validateIndexOrAliasName
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand
import org.opensearch.cluster.routing.allocation.decider.Decision
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.Tuple
import org.opensearch.common.settings.Settings
import org.opensearch.index.shard.DocsStats
import org.opensearch.index.store.StoreStats
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.util.getIntervalFromManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.util.getManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.util.getNodeFreeMemoryAfterShrink
import org.opensearch.indexmanagement.indexstatemanagement.util.getShardIdToNodeNameSet
import org.opensearch.indexmanagement.indexstatemanagement.util.getShrinkLockID
import org.opensearch.indexmanagement.indexstatemanagement.util.isIndexGreen
import org.opensearch.indexmanagement.indexstatemanagement.util.issueUpdateSettingsRequest
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.indices.InvalidIndexNameException
import org.opensearch.jobscheduler.spi.LockModel
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import java.lang.RuntimeException
import java.util.PriorityQueue
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.min
import kotlin.math.sqrt

@SuppressWarnings("TooManyFunctions")
class AttemptMoveShardsStep(private val action: ShrinkAction) : ShrinkStep(name, false, false, false) {

    @Suppress("ReturnCount")
    override suspend fun wrappedExecute(context: StepContext): AttemptMoveShardsStep {
        val client = context.client
        val indexName = context.metadata.index
        val shrinkTargetIndexName =
            compileTemplate(action.targetIndexTemplate, context.metadata, indexName + DEFAULT_TARGET_SUFFIX, context.scriptService)

        if (targetIndexNameIsInvalid(context.clusterService, shrinkTargetIndexName)) return this

        if (!isIndexGreen(client, indexName)) {
            info = mapOf("message" to INDEX_NOT_GREEN_MESSAGE)
            stepStatus = StepStatus.CONDITION_NOT_MET
            return this
        }

        if (shouldFailUnsafe(context.clusterService, indexName)) return this

        // If there is only one primary shard we complete the step and in getUpdatedManagedIndexMetadata will start a no-op
        val numOriginalShards = context.clusterService.state().metadata.indices[indexName].numberOfShards
        if (numOriginalShards == 1) {
            info = mapOf("message" to ONE_PRIMARY_SHARD_MESSAGE)
            stepStatus = StepStatus.COMPLETED
            return this
        }

        // Get stats on index size and docs
        val (statsStore, statsDocs, shardStats) = getIndexStats(indexName, client) ?: return this
        val indexSize = statsStore.sizeInBytes
        val numTargetShards = getNumTargetShards(numOriginalShards, indexSize)

        if (shouldFailTooManyDocuments(statsDocs, numTargetShards)) return this

        val originalIndexSettings = getOriginalSettings(indexName, context.clusterService)

        // get the nodes with enough memory in increasing order of free space
        val suitableNodes = findSuitableNodes(context, shardStats, indexSize)

        // Get the job interval to use in determining the lock length
        val interval = getJobIntervalSeconds(context.metadata.indexUuid, client)
        // iterate through the nodes and try to acquire a lock on one
        val (lock, nodeName) = acquireLockFromNodeList(context.lockService, suitableNodes, interval, indexName) ?: return this
        shrinkActionProperties = ShrinkActionProperties(
            nodeName,
            shrinkTargetIndexName,
            numTargetShards,
            lock.primaryTerm,
            lock.seqNo,
            lock.lockTime.epochSecond,
            lock.lockDurationSeconds,
            originalIndexSettings
        )

        setToReadOnlyAndMoveIndexToNode(context, nodeName, lock)
        info = mapOf("message" to getSuccessMessage(nodeName))
        stepStatus = StepStatus.COMPLETED
        return this
    }

    override fun getGenericFailureMessage(): String = FAILURE_MESSAGE

    private suspend fun getIndexStats(indexName: String, client: Client): Triple<StoreStats, DocsStats, Array<ShardStats>>? {
        val statsRequest = IndicesStatsRequest().indices(indexName)
        val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil {
            stats(statsRequest, it)
        }
        val statsStore = statsResponse.total.store
        val statsDocs = statsResponse.total.docs
        val statsShards = statsResponse.shards
        if (statsStore == null || statsDocs == null || statsShards == null) {
            fail(FAILURE_MESSAGE, "Failed to move shards in shrink action as IndicesStatsResponse was missing some stats.")
            return null
        }
        return Triple(statsStore, statsDocs, statsShards)
    }

    // Gets the routing and write block setting of the index and returns it in a map of setting name to setting
    private fun getOriginalSettings(indexName: String, clusterService: ClusterService): Map<String, String> {
        val indexSettings = clusterService.state().metadata.index(indexName).settings
        val originalSettings = mutableMapOf<String, String>()
        indexSettings.get(ROUTING_SETTING)?.let { it -> originalSettings.put(ROUTING_SETTING, it) }
        indexSettings.get(SETTING_BLOCKS_WRITE)?.let { it -> originalSettings.put(SETTING_BLOCKS_WRITE, it) }
        return originalSettings
    }

    private fun compileTemplate(
        template: Script?,
        managedIndexMetaData: ManagedIndexMetaData,
        defaultValue: String,
        scriptService: ScriptService
    ): String {
        if (template == null) return defaultValue
        val contextMap = managedIndexMetaData.convertToMap().filterKeys { key ->
            key in ALLOWED_TEMPLATE_FIELDS
        }
        val compiledValue = scriptService.compile(template, TemplateScript.CONTEXT)
            .newInstance(template.params + mapOf("ctx" to contextMap))
            .execute()
        return compiledValue.ifBlank { defaultValue }
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
            fail(TOO_MANY_DOCS_FAILURE_MESSAGE, TOO_MANY_DOCS_FAILURE_MESSAGE)
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
            logger.info(UNSAFE_FAILURE_MESSAGE)
            fail(UNSAFE_FAILURE_MESSAGE)
            return true
        }
        return false
    }

    private fun targetIndexNameIsInvalid(clusterService: ClusterService, shrinkTargetIndexName: String): Boolean {
        val indexExists = clusterService.state().metadata.indices.containsKey(shrinkTargetIndexName)
        if (indexExists) {
            val indexExistsMessage = getIndexExistsMessage(shrinkTargetIndexName)
            fail(indexExistsMessage, indexExistsMessage)
            return true
        }
        val exceptionGenerator: (String, String) -> RuntimeException = { index_name, reason -> InvalidIndexNameException(index_name, reason) }
        // If the index name is invalid for any reason, this will throw an exception giving the reason why in the message.
        // That will be displayed to the user as the cause.
        validateIndexOrAliasName(shrinkTargetIndexName, exceptionGenerator)
        return false
    }

    private suspend fun setToReadOnlyAndMoveIndexToNode(stepContext: StepContext, node: String, lock: LockModel): Boolean {
        val updateSettings = Settings.builder()
            .put(SETTING_BLOCKS_WRITE, true)
            .put(ROUTING_SETTING, node)
            .build()
        val lockService = stepContext.lockService
        var response: AcknowledgedResponse? = null
        val isUpdateAcknowledged: Boolean
        try {
            response = issueUpdateSettingsRequest(stepContext.client, stepContext.metadata.index, updateSettings)
        } finally {
            isUpdateAcknowledged = response != null && response.isAcknowledged
            if (!isUpdateAcknowledged) {
                fail(UPDATE_FAILED_MESSAGE, UPDATE_FAILED_MESSAGE)
                val released: Boolean = lockService.suspendUntil { release(lock, it) }
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
        lockService: LockService,
        suitableNodes: List<String>,
        jobIntervalSeconds: Long?,
        indexName: String
    ): Pair<LockModel, String>? {
        for (nodeName in suitableNodes) {
            val lockID = getShrinkLockID(nodeName)
            val lock: LockModel? = lockService.suspendUntil {
                acquireLockWithId(INDEX_MANAGEMENT_INDEX, getShrinkLockDuration(jobIntervalSeconds), lockID, it)
            }
            if (lock != null) {
                return lock to nodeName
            }
        }
        logger.info("Shrink action could not find available node to shrink onto for index [$indexName].")
        info = mapOf("message" to NO_AVAILABLE_NODES_MESSAGE)
        stepStatus = StepStatus.CONDITION_NOT_MET
        return null
    }

    /*
     * Returns the list of node names for nodes with enough space to shrink to, in increasing order of space available
     */
    @SuppressWarnings("NestedBlockDepth", "ComplexMethod")
    private suspend fun findSuitableNodes(
        stepContext: StepContext,
        shardStats: Array<ShardStats>,
        indexSizeInBytes: Long
    ): List<String> {
        val nodesStatsReq = NodesStatsRequest().addMetric(FS_METRIC)
        val nodeStatsResponse: NodesStatsResponse = stepContext.client.admin().cluster().suspendUntil { nodesStats(nodesStatsReq, it) }
        val nodesList = nodeStatsResponse.nodes.filter { it.node.isDataNode }
        // Sort in increasing order of keys, in our case this is memory remaining
        val comparator = kotlin.Comparator { o1: Tuple<Long, String>, o2: Tuple<Long, String> -> o1.v1().compareTo(o2.v1()) }
        val nodesWithSpace = PriorityQueue(comparator)
        for (node in nodesList) {
            // Gets the amount of memory in the node which will be free below the high watermark level after adding 2*indexSizeInBytes,
            // as the source index is duplicated during the shrink
            val remainingMem = getNodeFreeMemoryAfterShrink(node, indexSizeInBytes, stepContext.clusterService.clusterSettings)
            if (remainingMem > 0L) {
                nodesWithSpace.add(Tuple(remainingMem, node.node.name))
            }
        }
        val shardIdToNodeList: Map<Int, Set<String>> = getShardIdToNodeNameSet(shardStats, stepContext.clusterService.state().nodes)
        val suitableNodes: ArrayList<String> = ArrayList()
        // For each node, do a dry run of moving all shards to the node to make sure that there aren't any other blockers
        // to the allocation.
        for (sizeNodeTuple in nodesWithSpace) {
            val targetNodeName = sizeNodeTuple.v2()
            val indexName = stepContext.metadata.index
            val clusterRerouteRequest = ClusterRerouteRequest().explain(true).dryRun(true)
            val requestedShardIds: MutableSet<Int> = mutableSetOf()
            for (shard in shardStats) {
                val shardId = shard.shardRouting.shardId()
                val currentShardNode = stepContext.clusterService.state().nodes[shard.shardRouting.currentNodeId()]
                // Don't attempt a dry run for shards which have a copy already on that node
                if (shardIdToNodeList[shardId.id]?.contains(targetNodeName) == true || requestedShardIds.contains(shardId.id)) continue
                clusterRerouteRequest.add(MoveAllocationCommand(indexName, shardId.id, currentShardNode.name, targetNodeName))
                requestedShardIds.add(shardId.id)
            }
            val clusterRerouteResponse: ClusterRerouteResponse =
                stepContext.client.admin().cluster().suspendUntil { reroute(clusterRerouteRequest, it) }
            val numYesDecisions = clusterRerouteResponse.explanations.explanations().count { it.decisions().type().equals((Decision.Type.YES)) }
            // Should be the same number of yes decisions as the number of requests
            if (numYesDecisions == requestedShardIds.size) {
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
        if (action.maxShardSize != null) {
            val maxShardSizeInBytes = action.maxShardSize.bytes
            // ceiling ensures that numTargetShards is never less than 1
            val minNumTargetShards = ceil(indexSize / maxShardSizeInBytes.toDouble()).toInt()
            // In order to not violate the max shard size condition, this value must be >= minNumTargetShards.
            // If that value doesn't exist, numOriginalShards will be returned
            return getMinFactorGreaterThan(numOriginalShards, minNumTargetShards)
        }
        // Shrink action validation requires that at least one of the above will not be null, but return numOriginalShards for completion
        return numOriginalShards
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
        const val FS_METRIC = "fs"
        const val ROUTING_SETTING = "index.routing.allocation.require._name"
        const val DEFAULT_TARGET_SUFFIX = "_shrunken"
        const val name = "attempt_move_shards_step"
        const val UPDATE_FAILED_MESSAGE = "Shrink failed because shard settings could not be updated."
        const val NO_AVAILABLE_NODES_MESSAGE =
            "There are no available nodes to move to to execute a shrink. Delaying until node becomes available."
        const val UNSAFE_FAILURE_MESSAGE = "Shrink failed because index has no replicas and force_unsafe is not set to true."
        const val ONE_PRIMARY_SHARD_MESSAGE = "Shrink action did not do anything because source index only has one primary shard."
        const val TOO_MANY_DOCS_FAILURE_MESSAGE = "Shrink failed because there would be too many documents on each target shard following the shrink."
        const val INDEX_NOT_GREEN_MESSAGE = "Shrink action cannot start moving shards as the index is not green."
        const val FAILURE_MESSAGE = "Shrink failed to start moving shards."
        private const val DEFAULT_LOCK_INTERVAL = 3L * 60L * 60L // Default lock interval is 3 hours in seconds
        private const val MILLISECONDS_IN_SECOND = 1000L
        const val THIRTY_SECONDS_IN_MILLIS = 30L * MILLISECONDS_IN_SECOND
        private const val JOB_INTERVAL_LOCK_MULTIPLIER = 3
        private const val LOCK_BUFFER_SECONDS = 1800
        private const val MAXIMUM_DOCS_PER_SHARD = 0x80000000 // The maximum number of documents per shard is 2^31
        fun getSuccessMessage(node: String) = "Successfully started moving the shards to $node."
        fun getIndexExistsMessage(newIndex: String) = "Shrink failed because $newIndex already exists."
        // If we couldn't get the job interval for the lock, use the default of 12 hours.
        // Lock is 3x + 30 minutes the job interval to allow the next step's execution to extend the lock without losing it.
        // If user sets maximum jitter, it could be 2x the job interval before the next step is executed.
        private fun getShrinkLockDuration(jobInterval: Long?) = jobInterval?.let { (it * JOB_INTERVAL_LOCK_MULTIPLIER) + LOCK_BUFFER_SECONDS }
            ?: DEFAULT_LOCK_INTERVAL
        private val ALLOWED_TEMPLATE_FIELDS = setOf("index", "indexUuid")
    }
}
