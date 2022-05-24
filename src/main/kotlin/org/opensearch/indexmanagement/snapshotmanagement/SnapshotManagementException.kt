/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement

import org.opensearch.OpenSearchException
import org.opensearch.repositories.RepositoryMissingException
import kotlin.reflect.KClass

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

    constructor(ex: Exception) : this(
        message = exceptionMsgMap[exceptionTypeMap[ex::class] ?: ExceptionKey.GENERAL],
        cause = ex
    )

    constructor(exKey: ExceptionKey, ex: Exception? = null) : this(
        message = exceptionMsgMap[exKey],
        exKey = exKey,
        cause = ex,
    )

    companion object {
        const val GENERAL_EXCEPTION_KEY = "exception"

        private val exceptionTypeMap: Map<KClass<out Exception>, ExceptionKey> = mapOf(
            RepositoryMissingException::class to ExceptionKey.REPO_MISSING
        )

        // User facing exception messages
        private val exceptionMsgMap: Map<ExceptionKey, String> = mapOf(
            ExceptionKey.GENERAL to "Caught exception while snapshot management runs. Please check the error log.",
            ExceptionKey.METADATA_INDEXING_FAILURE to "Failed to update metadata while snapshot management runs.",
            ExceptionKey.REPO_MISSING to "The repository provided is missing."
        )
    }
}
