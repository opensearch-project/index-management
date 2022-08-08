/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:Suppress("TooManyFunctions")

package org.opensearch.indexmanagement.indexstatemanagement.util

import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.cluster.node.stats.NodeStats
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.admin.indices.stats.ShardStats
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.LOCK_RESOURCE_NAME
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction.Companion.LOCK_RESOURCE_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptMoveShardsStep
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import org.opensearch.jobscheduler.spi.LockModel
import org.opensearch.jobscheduler.spi.utils.LockService
import java.lang.Exception
import java.time.Instant

suspend fun issueUpdateSettingsRequest(client: Client, indexName: String, settings: Settings): AcknowledgedResponse {
    return client.admin()
        .indices()
        .suspendUntil { updateSettings(UpdateSettingsRequest(settings, indexName), it) }
}

suspend fun releaseShrinkLock(
    shrinkActionProperties: ShrinkActionProperties,
    lockService: LockService
): Boolean {
    val lock: LockModel = getShrinkLockModel(shrinkActionProperties)
    return lockService.suspendUntil { release(lock, it) }
}

suspend fun deleteShrinkLock(
    shrinkActionProperties: ShrinkActionProperties,
    lockService: LockService
): Boolean {
    val lockID = getShrinkLockID(shrinkActionProperties.nodeName)
    return lockService.suspendUntil { deleteLock(lockID, it) }
}

suspend fun renewShrinkLock(
    shrinkActionProperties: ShrinkActionProperties,
    lockService: LockService,
    logger: Logger
): LockModel? {
    val lock: LockModel = getShrinkLockModel(shrinkActionProperties)
    return try {
        lockService.suspendUntil { renewLock(lock, it) }
    } catch (e: Exception) {
        logger.error("Failed to renew Shrink action lock on node [${shrinkActionProperties.nodeName}]: $e")
        null
    }
}

fun getShrinkLockModel(
    shrinkActionProperties: ShrinkActionProperties
): LockModel {
    return getShrinkLockModel(
        shrinkActionProperties.nodeName,
        INDEX_MANAGEMENT_INDEX,
        shrinkActionProperties.lockEpochSecond,
        shrinkActionProperties.lockPrimaryTerm,
        shrinkActionProperties.lockSeqNo,
        shrinkActionProperties.lockDurationSecond
    )
}

@SuppressWarnings("LongParameterList")
fun getShrinkLockModel(
    nodeName: String,
    jobIndexName: String,
    lockEpochSecond: Long,
    lockPrimaryTerm: Long,
    lockSeqNo: Long,
    lockDurationSecond: Long
): LockModel {
    val lockID = getShrinkLockID(nodeName)
    val lockCreationInstant: Instant = Instant.ofEpochSecond(lockEpochSecond)
    return LockModel(
        jobIndexName,
        lockID,
        lockCreationInstant,
        lockDurationSecond,
        false,
        lockSeqNo,
        lockPrimaryTerm
    )
}

// Returns copied ShrinkActionProperties with the details of the provided lock added in
fun getUpdatedShrinkActionProperties(shrinkActionProperties: ShrinkActionProperties, lock: LockModel): ShrinkActionProperties {
    return ShrinkActionProperties(
        shrinkActionProperties.nodeName,
        shrinkActionProperties.targetIndexName,
        shrinkActionProperties.targetNumShards,
        lock.primaryTerm,
        lock.seqNo,
        lock.lockTime.epochSecond,
        lock.lockDurationSeconds,
        shrinkActionProperties.originalIndexSettings
    )
}

fun getActionStartTime(managedIndexMetaData: ManagedIndexMetaData): Instant {
    val actionMetadata = managedIndexMetaData.actionMetaData
    // Return the action start time, or if that is null return now
    actionMetadata?.startTime?.let { return Instant.ofEpochMilli(it) }
    return Instant.now()
}

/*
 * For disk threshold, if the values are set as a percentage, the percent parameter will return a value and the bytes
 * parameter will return 0, and vice versa for when the values are set as bytes. This method provides a single place to
 * parse either and get the byte value back.
 */
@Suppress("MagicNumber")
fun getFreeBytesThresholdHigh(clusterSettings: ClusterSettings, totalNodeBytes: Long): Long {
    val diskThresholdSettings = DiskThresholdSettings(getDiskSettings(clusterSettings), clusterSettings)
    // Depending on how a user provided input, this setting may be a percentage or byte value
    val diskThresholdPercent = diskThresholdSettings.freeDiskThresholdHigh
    val diskThresholdBytes = diskThresholdSettings.freeBytesThresholdHigh
    // If the disk threshold is set as a percentage, use it and convert it to bytes
    return if (diskThresholdPercent > 0.001) {
        // If the user set value is 95%, diskThresholdPercent will be returned as 5% from the DiskThresholdSettings object
        ((diskThresholdPercent / 100) * totalNodeBytes).toLong()
    } else diskThresholdBytes.bytes
}

fun getDiskSettings(clusterSettings: ClusterSettings): Settings {
    return Settings.builder().put(
        CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.key,
        clusterSettings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING)
    ).put(
        CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.key,
        clusterSettings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING)
    ).put(
        CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.key,
        clusterSettings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING)
    ).build()
}

/*
 * Returns the amount of memory in the node which will be free below the high watermark level after adding 2*indexSizeInBytes, or -1
 * if adding 2*indexSizeInBytes goes over the high watermark threshold, or if nodeStats does not contain OsStats.
*/
fun getNodeFreeMemoryAfterShrink(node: NodeStats, indexSizeInBytes: Long, clusterSettings: ClusterSettings): Long {
    val fsStats = node.fs
    if (fsStats != null) {
        val diskSpaceLeftInNode = fsStats.total.free.bytes
        val totalNodeDisk = fsStats.total.total.bytes
        val freeBytesThresholdHigh = getFreeBytesThresholdHigh(clusterSettings, totalNodeDisk)
        // We require that a node has enough space to be below the high watermark disk level with an additional 2 * the index size free
        val requiredBytes = (2 * indexSizeInBytes) + freeBytesThresholdHigh
        if (diskSpaceLeftInNode > requiredBytes) {
            return diskSpaceLeftInNode - requiredBytes
        }
    }
    return -1L
}

suspend fun isIndexGreen(
    client: Client,
    indexName: String,
    timeout: TimeValue = TimeValue(AttemptMoveShardsStep.THIRTY_SECONDS_IN_MILLIS)
): Boolean {
    // get index health, waiting for a green status
    val healthReq = ClusterHealthRequest().indices(indexName).waitForGreenStatus().timeout(timeout)
    val response: ClusterHealthResponse = client.admin().cluster().suspendUntil { health(healthReq, it) }
    // The request was set to wait for green index, if the request timed out, the index never was green
    return !response.isTimedOut
}

suspend fun resetReadOnlyAndRouting(index: String, client: Client, originalSettings: Map<String, String>): Boolean {
    val allocationSettings = Settings.builder()
        .put(AttemptMoveShardsStep.ROUTING_SETTING, originalSettings[AttemptMoveShardsStep.ROUTING_SETTING])
        .put(IndexMetadata.SETTING_BLOCKS_WRITE, originalSettings[IndexMetadata.SETTING_BLOCKS_WRITE]).build()
    val response: AcknowledgedResponse = issueUpdateSettingsRequest(client, index, allocationSettings)
    if (!response.isAcknowledged) {
        return false
    }
    return true
}

fun getShrinkLockID(nodeName: String): String {
    return "$LOCK_RESOURCE_TYPE-$LOCK_RESOURCE_NAME-$nodeName"
}

// Creates a map of shardIds to the set of node names which the shard copies reside on. For example, with 2 replicas
// each shardId would have a set containing 3 node names, for the nodes of the primary and two replicas.
fun getShardIdToNodeNameSet(shardStats: Array<ShardStats>, nodes: DiscoveryNodes): Map<Int, Set<String>> {
    val shardIdToNodeList: MutableMap<Int, MutableSet<String>> = mutableMapOf()
    for (shard in shardStats) {
        val shardId = shard.shardRouting.shardId().id
        // If nodeName is null, then the nodes could have changed since the indicesStatsResponse, just skip adding it
        val nodeName: String = nodes[shard.shardRouting.currentNodeId()].name ?: continue
        if (shardIdToNodeList.containsKey(shardId)) {
            shardIdToNodeList[shardId]?.add(nodeName)
        } else {
            shardIdToNodeList[shardId] = mutableSetOf(nodeName)
        }
    }
    return shardIdToNodeList
}
