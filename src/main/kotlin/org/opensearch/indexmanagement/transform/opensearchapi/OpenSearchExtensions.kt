/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.opensearchapi

import kotlinx.coroutines.delay
import org.apache.logging.log4j.Logger
import org.opensearch.OpenSearchException
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.opensearchapi.isRetryable
import org.opensearch.indexmanagement.transform.util.TransformLockManager
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.TaskCancelledException
import java.util.regex.Pattern

/**
 * Timeout pattern used for checking the timeout message which is in unique format if the transform search timeout was set programmatically
 * Message pattern: https://github.com/sohami/OpenSearch/blob/main/server/src/main/java/org/opensearch/action/support/TimeoutTaskCancellationUtility.java#L66
 */
private val timeoutMessagePattern = Pattern.compile("cancelled task with reason: Cancellation timeout of (.*) is expired")

/**
 * Retries the given [block] of code as specified by the receiver [BackoffPolicy],
 * if [block] throws an [OpenSearchException] that is retriable (502, 503, 504 or 500 with message Time exceeded).
 *
 * If all retries fail the final exception will be rethrown. Exceptions caught during intermediate retries are
 * logged as warnings to [logger]. Similar to [org.opensearch.action.bulk.Retry], except these retries on
 * 502, 503, 504 error codes as well as when TaskCancelledException is being raised as cause. If the request is timeout, lock will be renewed
 *
 * @param logger - logger used to log intermediate failures
 * @param transformLockManager - lock manager that stores current lock used in order to renew the lock if the request timed out
 * @param retryOn - any additional [RestStatus] values that should be retried
 * @param block - the block of code to retry. This should be a suspend function.
 */
suspend fun <T> BackoffPolicy.retryTransformSearch(
    logger: Logger,
    transformLockManager: TransformLockManager,
    retryOn: List<RestStatus> = emptyList(),
    block: suspend (backoff: TimeValue) -> T
): T {
    val iter = iterator()
    var backoff: TimeValue = TimeValue.ZERO
    do {
        try {
            return block(backoff)
        } catch (e: OpenSearchException) {
            if (!iter.hasNext() || !isRetryable(e, retryOn)) {
                throw e
            }
            backoff = iter.next()
            logger.warn("Operation failed. Retrying in $backoff.", e)
            delay(backoff.millis)
            if (isTransformOperationTimedOut(e)) {
                // In the case of time out, renew the lock
                transformLockManager.renewLockForScheduledJob()
            }
        }
    } while (true)
}

fun isRetryable(
    ex: OpenSearchException,
    retryOn: List<RestStatus>,
) = ex.isRetryable() || isTransformOperationTimedOut(ex) || retryOn.contains(ex.status())

/**
 * Retries on 408 or on TaskCancelledException once the message matches the given pattern.
 * In that case, retry request with reduced size param and timeout param is set based on the lock expiration
 */
@Suppress("ReturnCount")
fun isTransformOperationTimedOut(ex: OpenSearchException): Boolean {
    if (RestStatus.REQUEST_TIMEOUT == ex.status()) {
        return true
    }
    if (ex.cause != null && ex.cause is TaskCancelledException) {
        return timeoutMessagePattern.matcher((ex.cause as TaskCancelledException).message).matches()
    }
    return false
}
