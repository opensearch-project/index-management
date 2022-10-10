/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.util

import org.opensearch.indexmanagement.opensearchapi.lockExpirationInSeconds
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.jobscheduler.spi.JobExecutionContext

class TransformContext(
    val transformLockManager: TransformLockManager,
    var pageSize: Int?
) {
    fun getMaxRequestTimeout(): Long {
        val maxRequestTimeout = transformLockManager.getLock()?.lockExpirationInSeconds()?.minus(60)
        // TODO - add log
        if (maxRequestTimeout == null || maxRequestTimeout < 0L) {
            throw IllegalArgumentException("asdased")
        }
        return maxRequestTimeout
    }

    companion object {
        suspend fun initTransformContext(transform: Transform, context: JobExecutionContext): TransformContext {
            return TransformContext(TransformLockManager.initTransformLockManager(transform, context), null)
        }
    }
}
