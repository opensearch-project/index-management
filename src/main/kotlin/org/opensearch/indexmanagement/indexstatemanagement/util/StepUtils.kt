package org.opensearch.indexmanagement.indexstatemanagement.util

import org.apache.logging.log4j.Logger
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.LockModel
import org.opensearch.transport.RemoteTransportException
import java.time.Instant

suspend fun issueUpdateSettingsRequest(client: Client, managedIndexMetaData: ManagedIndexMetaData, settings: Settings): AcknowledgedResponse {
    try {
        return client.admin()
            .indices()
            .suspendUntil { updateSettings(UpdateSettingsRequest(settings, managedIndexMetaData.index), it) }
    } catch (e: Exception) {
        throw e
    }
}

suspend fun releaseShrinkLock(
    managedIndexMetaData: ManagedIndexMetaData,
    context: JobExecutionContext,
    logger: Logger
) {
    try {
        val lock: LockModel = getShrinkLockModel(managedIndexMetaData, context)
        val released: Boolean = context.lockService.suspendUntil { release(lock, it) }
        if (!released) {
            logger.warn("Lock not released on failure")
        }
    } catch (e: RemoteTransportException) {
        throw e
    }
}

private fun getShrinkLockModel(
    managedIndexMetaData: ManagedIndexMetaData,
    context: JobExecutionContext
): LockModel {
    val resource: HashMap<String, String> = HashMap()
    val shrinkActionProperties = managedIndexMetaData.actionMetaData!!.actionProperties!!.shrinkActionProperties
    resource[WaitForMoveShardsStep.RESOURCE_NAME] = shrinkActionProperties!!.nodeName!!
    val lockCreationInstant: Instant = Instant.ofEpochSecond(shrinkActionProperties.lockEpochSecond!!)
    return LockModel(
        context.jobIndexName,
        context.jobId,
        WaitForMoveShardsStep.RESOURCE_TYPE,
        resource as Map<String, Any>?,
        lockCreationInstant,
        WaitForMoveShardsStep.MOVE_SHARDS_TIMEOUT_IN_SECONDS,
        false,
        shrinkActionProperties.lockSeqNo!!,
        shrinkActionProperties.lockPrimaryTerm!!
    )
}

public fun getActionStartTime(managedIndexMetaData: ManagedIndexMetaData): Instant {
    if (managedIndexMetaData.actionMetaData?.startTime == null) {
        return Instant.now()
    }

    return Instant.ofEpochMilli(managedIndexMetaData.actionMetaData.startTime)
}
