/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.engine.states

import org.opensearch.indexmanagement.snapshotmanagement.engine.SMStateMachine
import org.opensearch.jobscheduler.spi.ScheduledJobParameter

/**
 * States contain the action to execute
 *
 * Execution metadata can be handled by the context object. e.g. [SMStateMachine]
 */
interface State {
    /**
     * In single [ScheduledJobParameter] run, this flag indicates
     * whether to continue executing next state
     */
    val continuous: Boolean

    suspend fun execute(context: SMStateMachine): Result

    abstract class Result
}
