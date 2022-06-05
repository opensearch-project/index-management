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
        exKey = exKey,
        cause = ex,
    )

    companion object {
        // Customized user facing exception messages
        private val exceptionMsgMap: Map<ExceptionKey, String> = mapOf(
            ExceptionKey.GENERAL to "Caught exception while snapshot management runs. Please check the error log.",
            ExceptionKey.METADATA_INDEXING_FAILURE to "Failed to update metadata.",
            ExceptionKey.REPO_MISSING to "The repository provided is missing.",
        )

        fun wrap(ex: Exception, key: ExceptionKey? = null): SnapshotManagementException {
            var message: String = exceptionMsgMap[ExceptionKey.GENERAL]!!
            if (key != null) return SnapshotManagementException(key, ex)
            if (ex.message != null) message = ex.message!!
            return SnapshotManagementException(cause = ex, message = message)
        }
    }
}
