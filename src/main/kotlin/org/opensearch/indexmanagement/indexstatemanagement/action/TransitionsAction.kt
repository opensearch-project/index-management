/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.step.transition.AttemptTransitionStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class TransitionsAction(
    val transitions: List<Transition>,
    val indexMetadataProvider: IndexMetadataProvider
) : Action(name, -1) {

    private val attemptTransitionStep = AttemptTransitionStep(this)
    private val steps = listOf(attemptTransitionStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(context: StepContext): Step {
        return attemptTransitionStep
    }

    companion object {
        const val name = "transition"
    }
}
