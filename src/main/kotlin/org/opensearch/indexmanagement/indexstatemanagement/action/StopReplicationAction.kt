/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.step.stopreplication.AttemptStopReplicationStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

/**
 * ISM action to stop replication on indices replicated on a follower cluster.
 */
class StopReplicationAction(
    index: Int,
) : Action(name, index) {
    companion object {
        const val name = "stop_replication"
    }

    private val attemptStopReplicationStep = AttemptStopReplicationStep()

    private val steps = listOf(attemptStopReplicationStep)

    override fun getStepToExecute(context: StepContext): Step = attemptStopReplicationStep

    override fun getSteps(): List<Step> = steps
}
