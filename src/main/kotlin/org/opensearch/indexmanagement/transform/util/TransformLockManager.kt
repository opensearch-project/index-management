package org.opensearch.indexmanagement.transform.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.opensearchapi.lockExpirationInSeconds
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.settings.TransformSettings
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.LockModel

class TransformLockManager(
    private val transform: Transform,
    private val context: JobExecutionContext,
    private val backoffPolicy: BackoffPolicy = BackoffPolicy.exponentialBackoff(
        TimeValue.timeValueMillis(TransformSettings.DEFAULT_RENEW_LOCK_RETRY_DELAY),
        TransformSettings.DEFAULT_RENEW_LOCK_RETRY_COUNT
    )
) {
    private val logger = LogManager.getLogger(javaClass)
    private var lock: LockModel? = null
    fun getLock() = lock

    /**
     * Util method to attempt to renew the requested lock using the backoff policy.
     * If failed to renew the lock using backoff policy will return a null lock otherwise returns renewed lock.
     */
    suspend fun renewLockForScheduledJob(): LockModel? {
        var updatedLock: LockModel? = null
        try {
            backoffPolicy.retry(logger) {
                updatedLock = context.lockService.suspendUntil { renewLock(lock, it) }
            }
        } catch (e: Exception) {
            logger.warn("Failed trying to renew lock on $lock, releasing the existing lock", e)
        }

        if (updatedLock == null) {
            releaseLockForScheduledJob()
        }
        lock = updatedLock
        return lock
    }

    /**
     * Util method to attempt to release the requested lock.
     * Returns a boolean of the success of the release lock
     */
    suspend fun releaseLockForScheduledJob(): Boolean {
        var released = false
        try {
            released = context.lockService.suspendUntil { release(lock, it) }
            if (!released) {
                logger.warn("Could not release lock for job ${lock!!.jobId}")
            }
        } catch (e: Exception) {
            logger.error("Failed to release lock for job ${lock!!.jobId}", e)
        }
        return released
    }

    /**
     * Util method to attempt to get the lock on the requested scheduled job using the backoff policy.
     * If failed to acquire the lock using backoff policy will return a null lock otherwise returns acquired lock.
     */
    private suspend fun acquireLockForScheduledJob() {
        try {
            // acquireLock will attempt to create the lock index if needed and then read/create a lock. This is purely for internal purposes
            // and should not need the role's context to run
            backoffPolicy.retry(logger) {
                lock = context.lockService.suspendUntil { acquireLock(transform, context, it) }
            }
        } catch (e: Exception) {
            logger.error("Failed to acquireLock for job ${transform.name}", e)
        }
    }

    companion object {

        suspend fun initTransformLockManager(
            transform: Transform,
            context: JobExecutionContext
        ): TransformLockManager {
            val transformLockManager = TransformLockManager(transform, context)
            transformLockManager.acquireLockForScheduledJob()
            return transformLockManager
        }
    }
}
