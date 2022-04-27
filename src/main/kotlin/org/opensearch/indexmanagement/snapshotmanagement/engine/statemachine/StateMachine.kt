/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

interface StateMachine {
    suspend fun next()
}

/**
 * Represent un-retryable exception during state machine execution
 */
class StateMachineException(message: String, cause: Throwable? = null) : Exception(message, cause)

/**
 *
 */
class UpdateMetadataException(message: String? = null, cause: Throwable? = null) : Exception(message, cause)
