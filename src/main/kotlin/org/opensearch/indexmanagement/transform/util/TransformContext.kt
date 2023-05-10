/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.util

/**
 * Context initialized on each transform execution
 */
class TransformContext(
    val transformLockManager: TransformLockManager,
    val targetDateFieldMappings: Map<String, Any>,
    var lastSuccessfulPageSize: Int? = null,
) {
    fun getMaxRequestTimeoutInSeconds(): Long? {
        // Lock timeout must be greater than LOCK_BUFFER
        var maxRequestTimeout = transformLockManager.lockExpirationInSeconds()?.minus(LOCK_BUFFER_SECONDS)
        // Do not set invalid timeout
        if (maxRequestTimeout != null && maxRequestTimeout < 0) {
            return null
        }
        return maxRequestTimeout
    }

    fun getTargetIndexDateFieldMappings() = targetDateFieldMappings

    suspend fun renewLockForLongSearch(timeSpentOnSearch: Long) {
        transformLockManager.renewLockForLongSearch(timeSpentOnSearch)
    }

    companion object {
        private const val LOCK_BUFFER_SECONDS = 60
    }
}
