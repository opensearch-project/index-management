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
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.LockModel
import java.lang.Exception
import java.time.Instant

suspend fun issueUpdateSettingsRequest(client: Client, indexName: String, settings: Settings): AcknowledgedResponse {
    return client.admin()
        .indices()
        .suspendUntil { updateSettings(UpdateSettingsRequest(settings, indexName), it) }
}

suspend fun releaseShrinkLock(
    shrinkActionProperties: ShrinkActionProperties,
    jobExecutionContext: JobExecutionContext,
    logger: Logger
) {
    val lock: LockModel = getShrinkLockModel(shrinkActionProperties, jobExecutionContext)
    val released: Boolean = jobExecutionContext.lockService.suspendUntil { release(lock, it) }
    if (!released) {
        logger.error("Failed to release Shrink action lock on node [${shrinkActionProperties.nodeName}]")
    }
}

suspend fun releaseShrinkLock(
    lock: LockModel,
    jobExecutionContext: JobExecutionContext,
    logger: Logger
) {
    val released: Boolean = jobExecutionContext.lockService.suspendUntil { release(lock, it) }
    if (!released) {
        logger.error("Failed to release Shrink action lock on node [${lock.resource[AttemptMoveShardsStep.RESOURCE_NAME] as String}]")
    }
}

suspend fun renewShrinkLock(
    shrinkActionProperties: ShrinkActionProperties,
    jobExecutionContext: JobExecutionContext,
    logger: Logger
): LockModel? {
    val lock: LockModel = getShrinkLockModel(shrinkActionProperties, jobExecutionContext)
    println(lock.lockDurationSeconds)
    return try {
        jobExecutionContext.lockService.suspendUntil { renewLock(lock, it) }
    } catch (e: Exception) {
        logger.error("Failed to renew Shrink action lock on node [${shrinkActionProperties.nodeName}]: $e")
        null
    }
}

fun getShrinkLockModel(
    shrinkActionProperties: ShrinkActionProperties,
    jobExecutionContext: JobExecutionContext
): LockModel {
    return getShrinkLockModel(
        shrinkActionProperties.nodeName,
        jobExecutionContext.jobIndexName,
        jobExecutionContext.jobId,
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
    jobId: String,
    lockEpochSecond: Long,
    lockPrimaryTerm: Long,
    lockSeqNo: Long,
    lockDurationSecond: Long
): LockModel {
    val resource: HashMap<String, String> = HashMap()
    resource[WaitForMoveShardsStep.RESOURCE_NAME] = nodeName
    val lockCreationInstant: Instant = Instant.ofEpochSecond(lockEpochSecond)
    return LockModel(
        jobIndexName,
        jobId,
        WaitForMoveShardsStep.RESOURCE_TYPE,
        resource as Map<String, Any>?,
        lockCreationInstant,
        lockDurationSecond,
        false,
        lockSeqNo,
        lockPrimaryTerm
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
fun getFreeBytesThresholdHigh(settings: Settings, clusterSettings: ClusterSettings?, totalNodeBytes: Long): Long {
    val diskThresholdSettings = DiskThresholdSettings(settings, clusterSettings)
    // Depending on how a user provided input, this setting may be a percentage or byte value
    val diskThresholdPercent = diskThresholdSettings.freeDiskThresholdHigh
    val diskThresholdBytes = diskThresholdSettings.freeBytesThresholdHigh
    // If the disk threshold is set as a percentage, use it and convert it to bytes. If
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

suspend fun isIndexGreen(client: Client, indexName: String): Boolean {
    // get index health, waiting for a green status
    val healthReq = ClusterHealthRequest().indices(indexName).waitForGreenStatus()
    val response: ClusterHealthResponse = client.admin().cluster().suspendUntil { health(healthReq, it) }
    // The request was set to wait for green index, if the request timed out, the index never was green
    return !response.isTimedOut
}

suspend fun clearReadOnlyAndRouting(index: String, client: Client): Boolean {
    val allocationSettings = Settings.builder().putNull(AttemptMoveShardsStep.ROUTING_SETTING).putNull(IndexMetadata.SETTING_BLOCKS_WRITE).build()
    val response: AcknowledgedResponse = issueUpdateSettingsRequest(client, index, allocationSettings)
    if (!response.isAcknowledged) {
        return false
    }
    return true
}
