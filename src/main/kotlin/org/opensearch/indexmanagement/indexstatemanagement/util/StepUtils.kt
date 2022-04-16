/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.util

import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.cluster.node.stats.NodeStats
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings
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
    lockService: LockService,
    logger: Logger
) {
    val lock: LockModel = getShrinkLockModel(shrinkActionProperties)
    val released: Boolean = lockService.suspendUntil { release(lock, it) }
    if (!released) {
        logger.error("Failed to release Shrink action lock on node [${shrinkActionProperties.nodeName}]")
    }
}

suspend fun deleteShrinkLock(
    shrinkActionProperties: ShrinkActionProperties,
    lockService: LockService,
    logger: Logger
) {
    val lockID = getShrinkLockID(shrinkActionProperties.nodeName)
    val deleted: Boolean = lockService.suspendUntil { deleteLock(lockID, it) }
    if (!deleted) {
        logger.error("Failed to delete Shrink action lock on node [${shrinkActionProperties.nodeName}]")
    }
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
fun getFreeBytesThresholdHigh(settings: Settings, clusterSettings: ClusterSettings?, totalNodeBytes: Long): Long {
    val diskThresholdSettings = DiskThresholdSettings(settings, clusterSettings)
    // Depending on how a user provided input, this setting may be a percentage or byte value
    val diskThresholdPercent = diskThresholdSettings.freeDiskThresholdHigh
    val diskThresholdBytes = diskThresholdSettings.freeBytesThresholdHigh
    // If the disk threshold is set as a percentage, use it and convert it to bytes
    return if (diskThresholdPercent > 0.001) {
        // If the user set value is 95%, diskThresholdPercent will be returned as 5% from the DiskThresholdSettings object
        ((diskThresholdPercent / 100) * totalNodeBytes).toLong()
    } else diskThresholdBytes.bytes
}

/*
 * Returns the amount of memory in the node which will be free below the high watermark level after adding 2*indexSizeInBytes, or -1
 * if adding 2*indexSizeInBytes goes over the high watermark threshold, or if nodeStats does not contain OsStats.
*/
fun getNodeFreeMemoryAfterShrink(node: NodeStats, indexSizeInBytes: Long, settings: Settings, clusterSettings: ClusterSettings?): Long {
    val osStats = node.os
    if (osStats != null) {
        val memLeftInNode = osStats.mem.free.bytes
        val totalNodeMem = osStats.mem.total.bytes
        val freeBytesThresholdHigh = getFreeBytesThresholdHigh(settings, clusterSettings, totalNodeMem)
        // We require that a node has enough space to be below the high watermark disk level with an additional 2 * the index size free
        val requiredBytes = (2 * indexSizeInBytes) + freeBytesThresholdHigh
        if (memLeftInNode > requiredBytes) {
            return memLeftInNode - requiredBytes
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
