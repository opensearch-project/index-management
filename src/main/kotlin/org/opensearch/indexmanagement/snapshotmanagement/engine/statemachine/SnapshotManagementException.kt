/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

/**
 * Centralized place to hold different failures specific to snapshot management
 */
class SnapshotManagementException(
    val code: ExceptionCode,
    cause: Throwable? = null
) : RuntimeException(cause) {

    enum class ExceptionCode {
        GENERAL,
        API_CALLING,
        METADATA_UPDATE,
    }

    constructor(ex: Exception) : this(ExceptionCode.GENERAL, ex)

    companion object {
        private val exceptionMsgMap: Map<ExceptionCode, String> = mapOf(
            ExceptionCode.GENERAL to "Caught exception while snapshot management is running. Please check the error log.",
            ExceptionCode.API_CALLING to "Undetermined about whether the last snapshot has been created, moving to next.",
            ExceptionCode.METADATA_UPDATE to "Caught exception when updating snapshot management metadata.",
        )

        fun SnapshotManagementException.getExceptionMsg(): String {
            return exceptionMsgMap[code] ?: "No exception message provided for $code."
        }
    }
}
