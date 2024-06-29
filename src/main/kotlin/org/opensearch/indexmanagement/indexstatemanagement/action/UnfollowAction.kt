/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.step.unfollow.AttemptUnfollowStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

/**
 * ISM action to stop replication on indices replicated on a follower cluster.
 */
class UnfollowAction(
    index: Int,
) : Action(name, index) {
    companion object {
        const val name = "unfollow"
    }

    private val attemptUnfollowStep = AttemptUnfollowStep()

    private val steps = listOf(attemptUnfollowStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptUnfollowStep
    }

    override fun getSteps(): List<Step> = steps
}
