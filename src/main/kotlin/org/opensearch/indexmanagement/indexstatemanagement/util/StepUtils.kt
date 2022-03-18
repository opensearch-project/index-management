/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.util

import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ShrinkActionProperties
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.LockModel
import java.time.Instant

suspend fun issueUpdateSettingsRequest(client: Client, managedIndexMetaData: ManagedIndexMetaData, settings: Settings): AcknowledgedResponse {
    return client.admin()
        .indices()
        .suspendUntil { updateSettings(UpdateSettingsRequest(settings, managedIndexMetaData.index), it) }
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
