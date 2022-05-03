/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

import org.opensearch.indexmanagement.snapshotmanagement.engine.states.SMState

/**
 * Context for state machine execution
 */
abstract class StateMachine {

    abstract var currentState: SMState

    abstract suspend fun next()
}
