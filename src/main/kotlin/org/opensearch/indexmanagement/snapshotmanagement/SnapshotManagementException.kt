/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.opensearch.OpenSearchException

/**
 * Wrap snapshot management related exception with a user facing error message
 *
 * @param exKey: represents one type of exception that may not already exist
 *  Each exception key should have a corresponding user facing message
 */
class SnapshotManagementException(
    val exKey: ExceptionKey? = null,
    cause: Throwable? = null,
    message: String? = null,
) : OpenSearchException(message, cause) {

    enum class ExceptionKey {
        GENERAL,
        METADATA_INDEXING_FAILURE,
        REPO_MISSING,
    }

    constructor(exKey: ExceptionKey, ex: Exception? = null) : this(
        message = exceptionMsgMap[exKey],
        cause = ex,
        exKey = exKey,
    )

    companion object {
        // Customized user facing exception messages
        private val exceptionMsgMap: Map<ExceptionKey, String> = mapOf(
            ExceptionKey.GENERAL to "Caught exception while snapshot management runs. Please check the error log.",
            ExceptionKey.METADATA_INDEXING_FAILURE to "Failed to update metadata.",
            ExceptionKey.REPO_MISSING to "The repository provided is missing.",
        )

        /**
         * Wrap an exception in SnapshotManagementException
         *
         * If you want to define a user facing exception message, you need to define an ExceptionKey
         *  with customized message. And wrap with the defined exceptionKey.
         */
        @Suppress("ReturnCount")
        fun getUserErrorMessage(ex: Exception?, key: ExceptionKey? = null): SnapshotManagementException {
            if (ex is SnapshotManagementException) return ex
            var message = "Caught exception while snapshot management runs. Please check the error log." // exceptionMsgMap[ExceptionKey.GENERAL]!!
            if (key != null) return SnapshotManagementException(key, ex)
            val exMsg = ex?.message
            if (exMsg != null) message = exMsg
            return SnapshotManagementException(cause = ex, message = message)
        }
    }
}
