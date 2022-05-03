/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.statemachine

abstract class StateMachine {
    abstract var currentState: SMState
    abstract suspend fun next()
}
