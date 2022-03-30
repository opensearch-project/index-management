/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.util

import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.LockModel
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
        logger.warn("Lock not released on failure")
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
        shrinkActionProperties.lockSeqNo
    )
}

@SuppressWarnings("LongParameterList")
fun getShrinkLockModel(
    nodeName: String,
    jobIndexName: String,
    jobId: String,
    lockEpochSecond: Long,
    lockPrimaryTerm: Long,
    lockSeqNo: Long
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
        WaitForMoveShardsStep.MOVE_SHARDS_TIMEOUT_IN_SECONDS,
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

suspend fun isIndexGreen(client: Client, indexName: String): Boolean {
    // get index health, waiting for a green status
    val healthReq = ClusterHealthRequest().indices(indexName).waitForGreenStatus()
    val response: ClusterHealthResponse = client.admin().cluster().suspendUntil { health(healthReq, it) }
    // The request was set to wait for green index, if the request timed out, the index never was green
    return !response.isTimedOut
}
