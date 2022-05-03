/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import org.opensearch.indexmanagement.snapshotmanagement.formatUserMsg
import org.opensearch.repositories.RepositoryMissingException
import kotlin.reflect.KClass

/**
 * Centralized place to hold different failures specific to snapshot management
 */
class StateMachineException(
    val code: ExceptionCode,
    cause: Throwable? = null
) : RuntimeException(cause) {

    enum class ExceptionCode {
        GENERAL,
        ATOMIC,
        METADATA_UPDATE,
        REPO_MISSING,
    }

    constructor(ex: Exception) : this(
        exceptionTypeMap[ex::class] ?: ExceptionCode.GENERAL, ex
    )

    companion object {
        private val exceptionTypeMap: Map<KClass<out Exception>, ExceptionCode> = mapOf(
            RepositoryMissingException::class to ExceptionCode.REPO_MISSING
        )

        private val exceptionMsgMap: Map<ExceptionCode, String> = mapOf(
            ExceptionCode.GENERAL to "Caught exception while snapshot management is running. Please check the error log.",
            ExceptionCode.ATOMIC to "Undetermined about whether the last snapshot operation has fully finished.",
            ExceptionCode.METADATA_UPDATE to "Caught exception when updating snapshot management metadata.",
            ExceptionCode.REPO_MISSING to "The repository provided is missing."
        )

        fun StateMachineException.getUserMsg(): String {
            val userMsg = exceptionMsgMap[code] ?: "No exception message provided for $code."
            return formatUserMsg(userMsg)
        }
    }
}
