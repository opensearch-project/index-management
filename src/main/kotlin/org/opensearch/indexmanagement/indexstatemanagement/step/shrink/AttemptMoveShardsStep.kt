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
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.Tuple
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ShrinkActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ActionProperties
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.ShrinkActionProperties
import org.opensearch.indexmanagement.indexstatemanagement.model.managedindexmetadata.StepMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.util.issueUpdateSettingsRequest
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.jobscheduler.repackage.com.cronutils.utils.VisibleForTesting
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.LockModel
import org.opensearch.transport.RemoteTransportException
import java.lang.Exception
import java.util.PriorityQueue
import kotlin.collections.HashMap
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.min
import kotlin.math.sqrt

@SuppressWarnings("TooManyFunctions")
class AttemptMoveShardsStep(
    val clusterService: ClusterService,
    val client: Client,
    val config: ShrinkActionConfig,
    managedIndexMetaData: ManagedIndexMetaData,
    val context: JobExecutionContext
) : Step(name, managedIndexMetaData) {
    private val logger = LogManager.getLogger(javaClass)
    private var stepStatus = StepStatus.STARTING
    private var info: Map<String, Any>? = null
    private var numTargetShards: Int? = null
    private var numOriginalShards: Int? = null
    private var nodeName: String? = null
    private var shrinkTargetIndexName: String? = null
    private var lock: LockModel? = null
    override fun isIdempotent() = false

    @Suppress("TooGenericExceptionCaught", "ComplexMethod", "ReturnCount")
    override suspend fun execute(): AttemptMoveShardsStep {
        try {
            // get cluster health
            logger.info("starting move shards step")
            logger.info("index name is ${managedIndexMetaData.index}")
            val healthReq = ClusterHealthRequest().indices(managedIndexMetaData.index).waitForGreenStatus()
            val response: ClusterHealthResponse = client.admin().cluster().suspendUntil { health(healthReq, it) }
            logger.info("Got past cluster health step.")
            // check status of cluster health
            if (response.isTimedOut) {
                info = mapOf("message" to "Step failed because of a request timeout.")
                stepStatus = StepStatus.CONDITION_NOT_MET
                return this
            }
            logger.info("Got past real cluster health")

            // check whether the target index name is available.
            val indexNameSuffix = config.targetIndexSuffix ?: "_shrunken"
            shrinkTargetIndexName = managedIndexMetaData.index + indexNameSuffix
            val indexExists = clusterService.state().metadata.indices.containsKey(shrinkTargetIndexName)
            if (indexExists) {
                info = mapOf("message" to "Step failed because $shrinkTargetIndexName already exists")
                stepStatus = StepStatus.FAILED
                return this
            }

            // force_unsafe check
            val numReplicas = clusterService.state().metadata.indices[managedIndexMetaData.index].numberOfReplicas
            if (config.forceUnsafe != null && !config.forceUnsafe && numReplicas == 0) {
                info = mapOf("message" to "Step failed because force_unsafe is false and the number of replicas of $indexName is 0.")
                stepStatus = StepStatus.FAILED
                return this
            }
            // Get the number of primary shards in the index -- all will be active because index health is green
            numOriginalShards = clusterService.state().metadata.indices[managedIndexMetaData.index].numberOfShards
            if (numOriginalShards == 1) {
                info = mapOf("message" to "Action failed because the number of primary shards of the index is 1.")
                stepStatus = StepStatus.FAILED
                return this
            }
            // Get the size of the index
            val statsRequest = IndicesStatsRequest().indices(indexName)
            val statsResponse: IndicesStatsResponse = client.admin().indices().suspendUntil {
                stats(statsRequest, it)
            }
            val indexSize = statsResponse.total.getStore()!!.sizeInBytes

            // get the number of shards that the target index will have
            numTargetShards = getNumTargetShards(numOriginalShards!!, indexSize)

            // get the nodes with enough memory
            val suitableNodes = findSuitableNodes(statsResponse, indexSize, bufferPercentage)
            // iterate through the nodes and try to acquire a lock on those nodes
            logger.info("attempting to acquire lock on node")
            lock = acquireLockOnNode(suitableNodes)
            if (lock == null) {
                info = mapOf("message" to "Step delayed because there are no suitable nodes on which to execute the shrink.")
                stepStatus = StepStatus.CONDITION_NOT_MET
                return this
            }
            // set index to read only
            setToReadOnly()
            // move the shards
            nodeName = lock!!.resource["node_name"] as String
            moveIndexToNode(nodeName!!)
            info = mapOf("message" to "Attempt move shard step completed.")
            stepStatus = StepStatus.COMPLETED
            return this
        } catch (e: RemoteTransportException) {
            info = mapOf("message" to "Step failed because of remote transport exception.")
            stepStatus = StepStatus.FAILED
            return this
        }
    }

    private suspend fun setToReadOnly() {
        val updateSettings = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build()
        issueUpdateAndUnlockIfFail(updateSettings, READ_ONLY_FAILED)
    }

    private suspend fun moveIndexToNode(node: String) {
        val allocationSettings = Settings.builder().put(ROUTING_SETTING, node).build()
        issueUpdateAndUnlockIfFail(allocationSettings, ALLOCATION_SETTING_FAILED)
    }

    private suspend fun issueUpdateAndUnlockIfFail(settings: Settings, failureMessage: String) {
        try {
            val response: AcknowledgedResponse = issueUpdateSettingsRequest(client, managedIndexMetaData, settings)
            if (!response.isAcknowledged) {
                stepStatus = StepStatus.FAILED
                info = mapOf("message" to failureMessage)
            }
        } catch (e: Exception) {
            handleException(e, failureMessage)
            context.lockService.suspendUntil<Boolean> { release(lock, it) }
        }
    }

    suspend fun acquireLockOnNode(suitableNodes: PriorityQueue<Tuple<Long, String>>): LockModel? {
        logger.info("in acquireLockOnNode")
        var lock: LockModel? = null
        for (tuple in suitableNodes) {
            val node = tuple.v2()
            logger.info("Attempting to acquire $node")
            val nodeResourceObject: HashMap<String, String> = HashMap()
            nodeResourceObject[RESOURCE_NAME] = node
            val lockTime = config.configTimeout?.timeout?.seconds ?: MOVE_SHARDS_TIMEOUT_IN_SECONDS
            logger.info("Acquisition request about to send")
            lock = context.lockService.suspendUntil<LockModel> {
                acquireLockOnResource(context, lockTime, RESOURCE_TYPE, nodeResourceObject as Map<String, Any>?, it)
            }
            logger.info("acquire request is sent")
            if (lock != null) {
                return lock
            }
        }
        return lock
    }

    @VisibleForTesting
    private suspend fun findSuitableNodes(indicesStatsResponse: IndicesStatsResponse, indexSize: Long, buffer: Long): PriorityQueue<Tuple<Long, String>> {
        logger.info("finding suitable nodes")
        val nodesStatsReq = NodesStatsRequest().addMetric("os")
        val nodeStatsResponse: NodesStatsResponse = client.admin().cluster().suspendUntil { nodesStats(nodesStatsReq, it) }
        val nodesList = nodeStatsResponse.nodes
        val comparator = kotlin.Comparator { o1: Tuple<Long, String>, o2: Tuple<Long, String> -> o1.v1().compareTo(o2.v1()) }
        val nodesWithSpace = PriorityQueue<Tuple<Long, String>>(comparator)
        for (node in nodesList) {
            val memLeftInNode = node.os!!.mem.free.bytes
            val totalNodeMem = node.os!!.mem.total.bytes
            val bufferSize = ByteSizeValue(buffer * totalNodeMem)
            val requiredBytes = (2 * indexSize) + bufferSize.bytes
            if (memLeftInNode > requiredBytes) {
                val memLeftAfterTransfer: Long = memLeftInNode - requiredBytes
                nodesWithSpace.add(Tuple(memLeftAfterTransfer, node.node.name))
            }
        }
        val suitableNodes = PriorityQueue<Tuple<Long, String>>(comparator)
        for (sizeNodeTuple in nodesWithSpace) {
            val movableShardIds = HashSet<Int>()
            for (shard in indicesStatsResponse.shards) {
                val shardId = shard.shardRouting.shardId()
                val currentShardNode = clusterService.state().nodes[shard.shardRouting.currentNodeId()]
                if (currentShardNode.name.equals(sizeNodeTuple.v2())) {
                    movableShardIds.add(shardId.id)
                } else {
                    val allocationCommand = MoveAllocationCommand(indexName, shardId.id, currentShardNode.name, sizeNodeTuple.v2())
                    val rerouteRequest = ClusterRerouteRequest().dryRun(true).add(allocationCommand)
                    val clusterRerouteResponse: ClusterRerouteResponse = client.admin().cluster().suspendUntil { reroute(rerouteRequest, it) }
                    if (clusterRerouteResponse.explanations.yesDecisionMessages.size != 0) {
                        movableShardIds.add(shardId.id)
                    }
                }
            }
            if (movableShardIds.size == numOriginalShards) {
                suitableNodes.add(sizeNodeTuple)
            }
        }

        logger.info("found ${nodesWithSpace.size} suitable nodes")
        return nodesWithSpace
    }

    @SuppressWarnings("ReturnCount")
    private fun getNumTargetShards(numOriginalShards: Int, indexSize: Long): Int {
        // case where user specifies a certain number of shards in the target index
        if (config.numNewShards != null) return getGreatestFactorLessThan(numOriginalShards, config.numNewShards)

        // case where user specifies a percentage decrease in the number of shards in the target index
        if (config.percentageDecrease != null) {
            val numTargetShards = floor((config.percentageDecrease) * numOriginalShards).toInt()
            return getGreatestFactorLessThan(numOriginalShards, numTargetShards)
        }
        // case where the user specifies a max shard size in the target index
        val maxShardSizeInBytes = config.maxShardSize!!.bytes
        // ensures that numTargetShards is never less than 1
        val minNumTargetShards = ceil(indexSize / maxShardSizeInBytes.toDouble()).toInt()
        return getMinFactorGreaterThan(numOriginalShards, minNumTargetShards)
    }

    @SuppressWarnings("ReturnCount")
    private fun getGreatestFactorLessThan(n: Int, k: Int): Int {
        if (k >= n) return n
        val bound: Int = min(floor(sqrt(n.toDouble())).toInt(), k)
        var greatestFactor = 1
        for (i in 2..bound + 1) {
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

    @SuppressWarnings("ReturnCount")
    private fun getMinFactorGreaterThan(n: Int, k: Int): Int {
        if (k >= n) {
            return n
        }
        for (i in k..n + 1) {
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

    override fun getUpdatedManagedIndexMetaData(currentMetaData: ManagedIndexMetaData): ManagedIndexMetaData {
        val currentActionMetaData = currentMetaData.actionMetaData
        logger.info("updating managed index metadata")
        return currentMetaData.copy(
            actionMetaData = currentActionMetaData?.copy(
                actionProperties = ActionProperties(
                    shrinkActionProperties = ShrinkActionProperties(
                        nodeName = nodeName,
                        targetIndexName = shrinkTargetIndexName,
                        targetNumShards = numTargetShards,
                        lockPrimaryTerm = lock?.primaryTerm,
                        lockSeqNo = lock?.seqNo,
                        lockEpochSecond = lock?.lockTime?.epochSecond
                    )
                )
            ),
            stepMetaData = StepMetaData(name, getStepStartTime().toEpochMilli(), stepStatus),
            transitionTo = null,
            info = info
        )
    }

    companion object {
        private const val ROUTING_SETTING = "index.routing.allocation.require._name"
        const val RESOURCE_NAME = "node_name"
        const val bufferPercentage = 0.05.toLong()
        const val MOVE_SHARDS_TIMEOUT_IN_SECONDS = 43200L // 12hrs in seconds
        const val name = "attempt_move_shards_step"
        const val RESOURCE_TYPE = "shrink"
        const val READ_ONLY_FAILED = "Failed to set index to read only."
        const val ALLOCATION_SETTING_FAILED = "Failed to start moving shards to the selected node."
    }
}
